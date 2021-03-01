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
package com.google.solutions.annotation;

import com.google.cloud.videointelligence.v1p3beta1.StreamingFeature;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.AnnotateImageResponse.Builder;
import com.google.cloud.vision.v1.Feature;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Annotation simulation class to test batching logic without incurring API costs.
 *
 * <p>It simulates the delay of calling the API and produces a single annotation.
 */
public class AnnotateFilesSimulatorDoFn
    extends DoFn<Iterable<GCSFileInfo>, KV<GCSFileInfo, GeneratedMessageV3>> {

  private static final long serialVersionUID = 1L;

  public AnnotateFilesSimulatorDoFn(
      List<Feature.Type> imageFeatures, List<StreamingFeature> videoFeatures) {
    /*
     * Feature types are ignored at the moment. But the simulation logic can be enhanced if needed to produce annotations
     * based on the requested features.
     */
  }

  @ProcessElement
  public void processElement(
      @Element Iterable<GCSFileInfo> fileInfos,
      OutputReceiver<KV<GCSFileInfo, GeneratedMessageV3>> out) {
    AnnotationPipeline.numberOfImageApiRequests.inc();
    try {
      /**
       * It creates a pattern similar to using the actual APIs with 16 requests per batch and two
       * features requested. If more sophisticated simulation is needed - externalize the values or
       * make these parameters dependent on batch size and the number of features requested.
       */
      Thread.sleep(500 + (new Random().nextInt(1000)));
    } catch (InterruptedException e) {
      // Do nothing
    }

    fileInfos.forEach(
        fileInfo -> {
          Builder responseBuilder = AnnotateImageResponse.newBuilder();
          responseBuilder
              .addLabelAnnotationsBuilder(0)
              .setDescription("Test")
              .setScore(.5F)
              .setTopicality(.6F)
              .setMid("/m/test");
          AnnotateImageResponse response = responseBuilder.build();
          out.output(KV.of(fileInfo, response));
        });
  }
}
