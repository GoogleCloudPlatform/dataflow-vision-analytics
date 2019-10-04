/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.solutions.ml.api.vision.common;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.AnnotateImageRequest;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CreateImageRequest {@link CreateImageReqest} batch the list of images with feature type and
 * create AnnotateImage Request
 */
public class CreateImageReqest
    extends DoFn<KV<String, Iterable<String>>, KV<String, AnnotateImageResponse>> {
  public static final Logger LOG = LoggerFactory.getLogger(CreateImageReqest.class);

  public static TupleTag<KV<String, AnnotateImageResponse>> successTag =
      new TupleTag<KV<String, AnnotateImageResponse>>() {};
  public static TupleTag<KV<String, TableRow>> failureTag = new TupleTag<KV<String, TableRow>>() {};

  private PCollectionView<List<Feature>> featureList;
  private ImageAnnotatorClient visionApiClient;
  private List<AnnotateImageRequest> requests;

  public CreateImageReqest(PCollectionView<List<Feature>> featureList) {
    this.featureList = featureList;
    this.requests = new ArrayList<>();
  }

  @StartBundle
  public void startBundle() {
    try {
      visionApiClient = ImageAnnotatorClient.create();
    } catch (IOException e) {
      LOG.error("Failed to create Vision API Service Client", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @FinishBundle
  public void finishBundle() {
    if (visionApiClient != null) {
      visionApiClient.close();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {

    Iterator<String> imgItr = c.element().getValue().iterator();
    AtomicInteger index = new AtomicInteger(0);
    String bucketName = c.element().getKey();
    List<Feature> features = c.sideInput(featureList);
    while (imgItr.hasNext()) {
      String imagePath = String.format("%s%s", bucketName, imgItr.next());
      Image image =
          Image.newBuilder()
              .setSource(ImageSource.newBuilder().setImageUri(imagePath).build())
              .build();
      AnnotateImageRequest.Builder request =
          AnnotateImageRequest.newBuilder().setImage(image).addAllFeatures(features);
      requests.add(request.build());
    }

    List<AnnotateImageResponse> responses =
        visionApiClient.batchAnnotateImages(requests).getResponsesList();

    for (AnnotateImageResponse res : responses) {
      if (res.hasError()) {
        ErrorMessageBuilder errorBuilder =
            ErrorMessageBuilder.newBuilder()
                .setErrorMessage("Error Processing Image Response")
                .setStackTrace(res.getError().getMessage())
                .setTimeStamp(VisionApiUtil.getTimeStamp())
                .build()
                .withTableRow(new TableRow());
        c.output(
            failureTag,
            KV.of(VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"), errorBuilder.tableRow()));
      } else {
        String imageName =
            requests.get(index.getAndIncrement()).getImage().getSource().getImageUri();
        c.output(KV.of(imageName, res));
      }
    }
  }
}
