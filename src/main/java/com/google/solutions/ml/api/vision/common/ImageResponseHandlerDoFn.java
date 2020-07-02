/*
 * Copyright (C) 2020 Google Inc.
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
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.CropHint;
import com.google.cloud.vision.v1.CropHintsAnnotation;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.cloud.vision.v1.FaceAnnotation;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessImageResponse {@link ImageResponseHandlerDoFn} class parses the image response for
 * specific annotation and using image response builder output the table and table row for BigQuery
 */
public class ImageResponseHandlerDoFn
    extends DoFn<KV<String, AnnotateImageResponse>, KV<String, TableRow>> {
  public static final Logger LOG = LoggerFactory.getLogger(ImageRequestDoFn.class);
  private final Counter numberOfAnnotationResponse =
      Metrics.counter(ImageResponseHandlerDoFn.class, "NumberOfAnnotationProcessed");

  @ProcessElement
  public void processElement(
      @Element KV<String, AnnotateImageResponse> element, MultiOutputReceiver out) {
    String imageName = element.getKey();
    AnnotateImageResponse imageResponse = element.getValue();
    imageResponse
        .getAllFields()
        .entrySet()
        .forEach(
            response -> {
              String key = response.getKey().getJsonName();
              try {
                switch (key) {
                  case "labelAnnotations":
                    numberOfAnnotationResponse.inc(imageResponse.getLabelAnnotationsCount());
                    for (EntityAnnotation annotation : imageResponse.getLabelAnnotationsList()) {
                      Row row = Util.transformLabelAnnotations(imageName, annotation);
                      out.get(Util.apiResponseSuccessElements)
                          .output(
                              KV.of(
                                  Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LABEL_ANNOTATION"),
                                  Util.toTableRow(row)));
                    }
                    break;
                  case "landmarkAnnotations":
                    numberOfAnnotationResponse.inc(imageResponse.getLandmarkAnnotationsCount());
                    for (EntityAnnotation annotation : imageResponse.getLandmarkAnnotationsList()) {
                      Row row = Util.transformLandmarkAnnotations(imageName, annotation);
                      out.get(Util.apiResponseSuccessElements)
                          .output(
                              KV.of(
                                  Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LANDMARK_ANNOTATION"),
                                  Util.toTableRow(row)));
                    }
                    break;
                  case "logoAnnotations":
                    numberOfAnnotationResponse.inc(imageResponse.getLogoAnnotationsCount());
                    for (EntityAnnotation annotation : imageResponse.getLogoAnnotationsList()) {
                      Row row = Util.transformLogoAnnotations(imageName, annotation);
                      out.get(Util.apiResponseSuccessElements)
                          .output(
                              KV.of(
                                  Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LOGO_ANNOTATION"),
                                  Util.toTableRow(row)));
                    }
                    break;
                  case "faceAnnotations":
                    numberOfAnnotationResponse.inc(imageResponse.getFaceAnnotationsCount());

                    for (FaceAnnotation annotation : imageResponse.getFaceAnnotationsList()) {
                      Row row = Util.transformFaceAnnotations(imageName, annotation);
                      out.get(Util.apiResponseSuccessElements)
                          .output(
                              KV.of(
                                  Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_FACE_ANNOTATION"),
                                  Util.toTableRow(row)));
                    }
                    break;
                  case "cropHintsAnnotation":
                    if (imageResponse.hasCropHintsAnnotation()) {
                      CropHintsAnnotation annotation = imageResponse.getCropHintsAnnotation();
                      for (CropHint crophint : annotation.getCropHintsList()) {
                        Row row = Util.transformCropHintsAnnotations(imageName, crophint);
                        out.get(Util.apiResponseSuccessElements)
                            .output(
                                KV.of(
                                    Util.BQ_TABLE_NAME_MAP.get(
                                        "BQ_TABLE_NAME_CROP_HINTS_ANNOTATION"),
                                    Util.toTableRow(row)));
                      }
                    }
                    break;

                  case "imagePropertiesAnnotation":
                    if (imageResponse.hasImagePropertiesAnnotation()) {
                      Row row =
                          Util.transformImagePropertiesAnnotations(
                              imageName, imageResponse.getImagePropertiesAnnotation());
                      out.get(Util.apiResponseSuccessElements)
                          .output(
                              KV.of(
                                  Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_IMAGE_PROP_ANNOTATION"),
                                  Util.toTableRow(row)));
                    }
                    if (imageResponse.hasCropHintsAnnotation()) {
                      CropHintsAnnotation annotation = imageResponse.getCropHintsAnnotation();
                      for (CropHint crophint : annotation.getCropHintsList()) {
                        Row row = Util.transformCropHintsAnnotations(imageName, crophint);
                        out.get(Util.apiResponseSuccessElements)
                            .output(
                                KV.of(
                                    Util.BQ_TABLE_NAME_MAP.get(
                                        "BQ_TABLE_NAME_CROP_HINTS_ANNOTATION"),
                                    Util.toTableRow(row)));
                      }
                    }
                    break;
                  default:
                    String errorMessage = String.format("Feature Type %s Not Supported", key);
                    LOG.error(Util.FEATURE_TYPE_NOT_SUPPORTED, key);
                    out.get(Util.apiResponseFailedElements)
                        .output(
                            KV.of(
                                imageName,
                                ErrorMessageBuilder.newBuilder()
                                    .setErrorMessage(errorMessage)
                                    .setFileName(imageName)
                                    .setTimeStamp(Util.getTimeStamp())
                                    .build()
                                    .withTableRow(new TableRow())));
                }
              } catch (Exception e) {
                LOG.error(
                    "Error '{}' processing response for file '{}'", e.getMessage(), imageName);
                out.get(Util.apiResponseFailedElements)
                    .output(
                        KV.of(
                            imageName,
                            ErrorMessageBuilder.newBuilder()
                                .setErrorMessage(e.getMessage())
                                .setFileName(imageName)
                                .setTimeStamp(Util.getTimeStamp())
                                .build()
                                .withTableRow(new TableRow())));
              }
            });
  }
}
