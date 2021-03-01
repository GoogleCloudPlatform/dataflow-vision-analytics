/*
 * Copyright 2021 Google LLC
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
package com.google.solutions.annotation.ml.vision;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.cloud.vision.v1.AnnotateImageRequest;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageSource;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.AnnotationPipeline;
import com.google.solutions.annotation.ml.BackOffUtils;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calls Google Cloud Vision API to annotate a batch of GCS files.
 *
 * <p>The GCS file URIs are provided in the incoming PCollection and should not exceed the limit
 * imposed by the API (maximum of 16 images per request).
 *
 * <p>The resulting PCollection contains key/value pair with the GCS file URI as the key and the API
 * response as the value.
 */
public class ImageAnnotator {

  public static final Logger LOG = LoggerFactory.getLogger(ImageAnnotator.class);

  private final List<Feature> featureList = new ArrayList<>();
  private final ImageAnnotatorClient client;

  public ImageAnnotator(List<Feature.Type> featureTypes) {
    if (featureTypes != null) {
      featureTypes.forEach(type -> featureList.add(Feature.newBuilder().setType(type).build()));
    }

    try {
      client = ImageAnnotatorClient.create();
    } catch (IOException e) {
      LOG.error("Failed to create Vision API Service Client: {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public void teardown() {
    if (client != null) {
      client.shutdownNow();
      try {
        int waitTime = 10;
        if (!client.awaitTermination(waitTime, TimeUnit.SECONDS)) {
          LOG.warn(
              "Failed to shutdown the annotation client after {} seconds. Closing client anyway.",
              waitTime);
        }
      } catch (InterruptedException e) {
        // Do nothing
      }
      client.close();
    }
  }

  public List<KV<GCSFileInfo, GeneratedMessageV3>> processFiles(Iterable<GCSFileInfo> fileInfos) {
    List<AnnotateImageRequest> requests = new ArrayList<>();

    Map<String, GCSFileInfo> uriToFileInfo = new HashMap<>();

    fileInfos.forEach(
        fileInfo -> {
          uriToFileInfo.put(fileInfo.getUri(), fileInfo);
          Image image =
              Image.newBuilder()
                  .setSource(ImageSource.newBuilder().setImageUri(fileInfo.getUri()).build())
                  .build();
          AnnotateImageRequest.Builder request =
              AnnotateImageRequest.newBuilder().setImage(image).addAllFeatures(featureList);
          requests.add(request.build());
        });

    List<AnnotateImageResponse> responses;

    ExponentialBackOff backoff = BackOffUtils.createBackOff();
    while (true) {
      try {
        AnnotationPipeline.numberOfImageApiRequests.inc();
        responses = client.batchAnnotateImages(requests).getResponsesList();
        break;
      } catch (ResourceExhaustedException e) {
        BackOffUtils.handleQuotaReachedException(backoff, e);
      }
    }

    int index = 0;
    List<KV<GCSFileInfo, GeneratedMessageV3>> result = new ArrayList<>();
    for (AnnotateImageResponse response : responses) {
      String uri = requests.get(index++).getImage().getSource().getImageUri();
      GCSFileInfo fileInfo = uriToFileInfo.get(uri);
      result.add(KV.of(fileInfo, response));
    }
    return result;
  }
}
