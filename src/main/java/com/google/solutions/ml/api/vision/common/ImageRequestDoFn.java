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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CreateImageRequest {@link ImageRequestDoFn} batch the list of images with feature type and create
 * AnnotateImage Request
 */
public class ImageRequestDoFn extends DoFn<List<String>, KV<String, AnnotateImageResponse>> {
  public static final Logger LOG = LoggerFactory.getLogger(ImageRequestDoFn.class);

  public static TupleTag<KV<String, AnnotateImageResponse>> successTag =
      new TupleTag<KV<String, AnnotateImageResponse>>() {};
  public static TupleTag<KV<String, TableRow>> failureTag = new TupleTag<KV<String, TableRow>>() {};

  private List<Feature> featureList = new ArrayList<>();
  private ImageAnnotatorClient visionApiClient;
  private final Counter numberOfRequest =
      Metrics.counter(ImageRequestDoFn.class, "NumberOfImageRequest");
  private final Counter numberOfResponse =
      Metrics.counter(ImageRequestDoFn.class, "NumberOfImageResponse");

  public ImageRequestDoFn(List<Feature.Type> featureTypes) {
    featureTypes.forEach(
        type -> {
          featureList.add(Feature.newBuilder().setType(type).build());
        });
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
    List<AnnotateImageRequest> requests = new ArrayList<>();
    List<String> imgList = c.element();

    AtomicInteger index = new AtomicInteger(0);
    imgList.forEach(
        img -> {
          Image image =
              Image.newBuilder()
                  .setSource(ImageSource.newBuilder().setImageUri(img).build())
                  .build();
          AnnotateImageRequest.Builder request =
              AnnotateImageRequest.newBuilder().setImage(image).addAllFeatures(featureList);
          requests.add(request.build());
        });

    List<AnnotateImageResponse> responses =
        visionApiClient.batchAnnotateImages(requests).getResponsesList();

    numberOfRequest.inc(requests.size());
    for (AnnotateImageResponse res : responses) {
      if (res.hasError()) {
        ErrorMessageBuilder errorBuilder =
            ErrorMessageBuilder.newBuilder()
                .setErrorMessage("Error Processing Image Response")
                .setStackTrace(res.getError().getMessage())
                .setTimeStamp(Util.getTimeStamp())
                .build()
                .withTableRow(new TableRow());
        LOG.error("Error {}", errorBuilder.toString());
        c.output(
            failureTag,
            KV.of(Util.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"), errorBuilder.tableRow()));
      } else {
        String imageName =
            requests.get(index.getAndIncrement()).getImage().getSource().getImageUri();
        numberOfResponse.inc(1);
        c.output(KV.of(imageName, res));
      }
    }
  }
}