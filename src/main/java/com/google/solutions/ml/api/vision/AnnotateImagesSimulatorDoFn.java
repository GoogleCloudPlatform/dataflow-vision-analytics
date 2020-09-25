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

import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.AnnotateImageResponse.Builder;
import com.google.cloud.vision.v1.Feature;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Image annotation simulation class to test batching logic without incurring Vision API costs.
 *
 * It simulates the delay of calling the API and produces a single annotation.
 */
public class AnnotateImagesSimulatorDoFn extends
    DoFn<Iterable<String>, KV<String, AnnotateImageResponse>> {

  private static final long serialVersionUID = 1L;

  public AnnotateImagesSimulatorDoFn(List<Feature.Type> featureTypes) {
    /*
     * Feature types are ignored at the moment. But the simulation logic can be enhanced if needed to produce annotations
     * based on the requested features.
     */
  }

  @ProcessElement
  public void processElement(@Element Iterable<String> imageUris,
      OutputReceiver<KV<String, AnnotateImageResponse>> out) {
    VisionAnalyticsPipeline.numberOfRequests.inc();
    try {
      Thread.sleep(500 + (new Random().nextInt(1000)));
    } catch (InterruptedException e) {
      // Do nothing
    }

    imageUris.forEach(
        imageUri -> {
          Builder responseBuilder = AnnotateImageResponse.newBuilder();
          responseBuilder.addLabelAnnotationsBuilder(0).setDescription("Test").setScore(.5F)
              .setTopicality(.6F).setMid("/m/test");
          AnnotateImageResponse response = responseBuilder.build();
          out.output(KV.of(imageUri, response));
        });
  }
}
