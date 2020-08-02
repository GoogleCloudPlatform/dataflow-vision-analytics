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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.CropHint;
import com.google.cloud.vision.v1.CropHintsAnnotation;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.solutions.ml.api.vision.VisionAnalyticsPipeline;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessImageResponse {@link ProcessImageResponseDoFn} class parses the image response for
 * specific annotation and using image response builder output the table and table row for BigQuery
 */
@SuppressWarnings("serial")
public class ProcessImageResponseDoFn
    extends DoFn<KV<String, AnnotateImageResponse>, KV<String, TableRow>> {
  private static final long serialVersionUID = 1L;
  public static final Logger LOG = LoggerFactory.getLogger(AnnotateImagesDoFn.class);

  private final Counter numberOfLabelAnnotations =
      Metrics.counter(ProcessImageResponseDoFn.class, "numberOfLabelAnnotations");
  private final Counter numberOfLandmarkAnnotations =
      Metrics.counter(ProcessImageResponseDoFn.class, "numberOfLandmarkAnnotations");
  private final Counter numberOfLogoAnnotations =
      Metrics.counter(ProcessImageResponseDoFn.class, "numberOfLogoAnnotations");
  private final Counter numberOfCropHintsAnnotations =
      Metrics.counter(ProcessImageResponseDoFn.class, "numberOfCropHintsAnnotations");
  private final Counter numberOfFaceAnnotations =
      Metrics.counter(ProcessImageResponseDoFn.class, "numberOfFaceAnnotations");
  private final Counter numberOfImageProperties =
      Metrics.counter(ProcessImageResponseDoFn.class, "numberOfImageProperties");

  @ProcessElement
  public void processElement(@Element KV<String, AnnotateImageResponse> element,
      OutputReceiver<KV<String,TableRow>> out) {
    String imageFileURI = element.getKey();
    AnnotateImageResponse annotationResponse = element.getValue();

    if (annotationResponse.hasError()) {
      // TODO: add metrics
      out.output(
          KV.of(
              Util.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
              Util.toTableRow(
                  Row.withSchema(Util.errorSchema)
                      .addValues(null, Util.getTimeStamp(),
                          annotationResponse.getError().toString(), null)
                      .build())));
      VisionAnalyticsPipeline.rejectedFiles.inc();
      return;
    }

    VisionAnalyticsPipeline.processedFiles.inc();

    annotationResponse
        .getAllFields()
        .entrySet()
        .forEach(
            response -> {
              String key = response.getKey().getJsonName();
              try {
                switch (key) {
                  case "labelAnnotations":
                    numberOfLabelAnnotations.inc(annotationResponse.getLabelAnnotationsCount());
                    for (EntityAnnotation annotation : annotationResponse
                        .getLabelAnnotationsList()) {
                      Row row = Util.transformLabelAnnotations(imageFileURI, annotation);
                      out.output(
                          KV.of(
                              Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LABEL_ANNOTATION"),
                              Util.toTableRow(row)));
                    }
                    break;
                  case "landmarkAnnotations":
                    numberOfLandmarkAnnotations
                        .inc(annotationResponse.getLandmarkAnnotationsCount());
                    for (EntityAnnotation annotation : annotationResponse
                        .getLandmarkAnnotationsList()) {
                      Row row = Util.transformLandmarkAnnotations(imageFileURI, annotation);
                      out.output(
                          KV.of(
                              Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LANDMARK_ANNOTATION"),
                              Util.toTableRow(row)));
                    }
                    break;
                  case "logoAnnotations":
                    numberOfLogoAnnotations.inc(annotationResponse.getLogoAnnotationsCount());
                    for (EntityAnnotation annotation : annotationResponse
                        .getLogoAnnotationsList()) {
                      Row row = Util.transformLogoAnnotations(imageFileURI, annotation);
                      out.output(
                          KV.of(
                              Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LOGO_ANNOTATION"),
                              Util.toTableRow(row)));
                    }
                    break;
                  case "faceAnnotations":
                    numberOfFaceAnnotations.inc(annotationResponse.getFaceAnnotationsCount());

                    for (FaceAnnotation annotation : annotationResponse.getFaceAnnotationsList()) {
                      Row row = Util.transformFaceAnnotations(imageFileURI, annotation);
                      out.output(
                          KV.of(
                              Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_FACE_ANNOTATION"),
                              Util.toTableRow(row)));
                    }
                    break;
                  case "cropHintsAnnotation":
                    if (annotationResponse.hasCropHintsAnnotation()) {
                      CropHintsAnnotation annotation = annotationResponse.getCropHintsAnnotation();
                      for (CropHint crophint : annotation.getCropHintsList()) {
                        numberOfCropHintsAnnotations.inc();
                        Row row = Util.transformCropHintsAnnotations(imageFileURI, crophint);
                        out.output(
                            KV.of(
                                Util.BQ_TABLE_NAME_MAP.get(
                                    "BQ_TABLE_NAME_CROP_HINTS_ANNOTATION"),
                                Util.toTableRow(row)));
                      }
                    }
                    break;

                  case "imagePropertiesAnnotation":
                    if (annotationResponse.hasImagePropertiesAnnotation()) {
                      numberOfImageProperties.inc();
                      Row row =
                          Util.transformImagePropertiesAnnotations(
                              imageFileURI, annotationResponse.getImagePropertiesAnnotation());
                      out
                          .output(
                              KV.of(
                                  Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_IMAGE_PROP_ANNOTATION"),
                                  Util.toTableRow(row)));
                    }
                    if (annotationResponse.hasCropHintsAnnotation()) {
                      CropHintsAnnotation annotation = annotationResponse.getCropHintsAnnotation();
                      for (CropHint crophint : annotation.getCropHintsList()) {
                        numberOfCropHintsAnnotations.inc();

                        Row row = Util.transformCropHintsAnnotations(imageFileURI, crophint);
                        out.output(
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
                    out.output(
                        KV.of(
                            Util.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
                            Util.toTableRow(
                                Row.withSchema(Util.errorSchema)
                                    .addValues(
                                        imageFileURI, Util.getTimeStamp(), errorMessage, null)
                                    .build())));
                }
              } catch (Exception e) {
                LOG.error(
                    "Error '{}' processing response for file '{}'", e.getMessage(), imageFileURI);
                out.output(
                    KV.of(
                        Util.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
                        Util.toTableRow(
                            Row.withSchema(Util.errorSchema)
                                .addValues(
                                    imageFileURI,
                                    Util.getTimeStamp(),
                                    e.getMessage(),
                                    ExceptionUtils.getStackTrace(e))
                                .build())));
              }
            });
  }
}
