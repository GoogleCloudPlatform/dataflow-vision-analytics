/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.solutions.ml.api.vision.common;

import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.AnnotateImageResponse.Builder;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.ImageSource;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * CreateImageRequest {@link AnnotateImagesSimulatorDoFn} batch the list of images with feature type
 * and create AnnotateImage Request
 */
@SuppressWarnings("serial")
public class AnnotateImagesSimulatorDoFn extends
    DoFn<Iterable<String>, KV<String, AnnotateImageResponse>> {

  private final List<Feature> featureList = new ArrayList<>();

  public AnnotateImagesSimulatorDoFn(List<Feature.Type> featureTypes) {
    featureTypes.forEach(
        type -> featureList.add(Feature.newBuilder().setType(type).build()));
  }

  @ProcessElement
  public void processElement(@Element Iterable<String> imageUris,
      OutputReceiver<KV<String, AnnotateImageResponse>> out) {

    try {
      Thread.sleep(300);
    } catch (InterruptedException e) {
      // Do nothing
    }

    imageUris.forEach(
        imageUri -> {
          Image image =
              Image.newBuilder()
                  .setSource(ImageSource.newBuilder().setImageUri(imageUri).build())
                  .build();
          Builder responseBuilder = AnnotateImageResponse.newBuilder();
          responseBuilder.addLabelAnnotationsBuilder(0).setDescription("Test").setScore(.5F)
              .setTopicality(.6F).setMid("/m/test");
          AnnotateImageResponse response = responseBuilder.build();
          out.output(KV.of(imageUri, response));
        });
  }
}
