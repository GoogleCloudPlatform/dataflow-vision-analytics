/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.solutions.ml.api.vision;

import com.google.cloud.vision.v1.AnnotateImageRequest;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calls Google Cloud Vision API to annotate a batch of GCS files.
 *
 * The GCS file URIs are provided in the incoming PCollection and should not exceed the limit
 * imposed by the API (maximum of 16 images per request).
 *
 * The resulting PCollection contains key/value pair with the GCS file URI as the key and the API
 * response as the value.
 */
public class AnnotateImagesDoFn extends DoFn<Iterable<String>, KV<String, AnnotateImageResponse>> {

  private static final long serialVersionUID = 1L;

  public static final Logger LOG = LoggerFactory.getLogger(AnnotateImagesDoFn.class);

  private final List<Feature> featureList = new ArrayList<>();
  private ImageAnnotatorClient visionApiClient;

  public AnnotateImagesDoFn(List<Feature.Type> featureTypes) {
    featureTypes.forEach(
        type -> featureList.add(Feature.newBuilder().setType(type).build()));
  }

  @Setup
  public void setupAPIClient() {
    try {
      visionApiClient = ImageAnnotatorClient.create();
    } catch (IOException e) {
      LOG.error("Failed to create Vision API Service Client: {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Teardown
  public void tearDownAPIClient() {
    if (visionApiClient != null) {
      visionApiClient.shutdownNow();
      try {
        int waitTime = 10;
        if (!visionApiClient.awaitTermination(waitTime, TimeUnit.SECONDS)) {
          LOG.warn(
              "Failed to shutdown the annotation client after {} seconds. Closing client anyway.",
              waitTime);
        }
      } catch (InterruptedException e) {
        // Do nothing
      }
      visionApiClient.close();
    }
  }

  @ProcessElement
  public void processElement(@Element Iterable<String> imageFileURIs,
      OutputReceiver<KV<String, AnnotateImageResponse>> out) {
    List<AnnotateImageRequest> requests = new ArrayList<>();

    imageFileURIs.forEach(
        imageUri -> {
          Image image =
              Image.newBuilder()
                  .setSource(ImageSource.newBuilder().setImageUri(imageUri).build())
                  .build();
          AnnotateImageRequest.Builder request =
              AnnotateImageRequest.newBuilder().setImage(image).addAllFeatures(featureList);
          requests.add(request.build());
        });

    List<AnnotateImageResponse> responses =
        visionApiClient.batchAnnotateImages(requests).getResponsesList();

    int index = 0;
    for (AnnotateImageResponse response : responses) {
      String imageUri = requests.get(index++).getImage().getSource().getImageUri();
      out.output(KV.of(imageUri, response));
    }
  }
}
