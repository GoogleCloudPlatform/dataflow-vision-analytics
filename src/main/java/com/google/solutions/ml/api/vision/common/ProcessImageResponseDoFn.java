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
import com.google.solutions.ml.api.vision.processor.AnnotationProcessor;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.VisionAnalyticsPipeline;
import java.util.Collection;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessImageResponse {@link ProcessImageResponseDoFn} class parses the image response for
 * specific annotation and using image response builder output the table and table row for BigQuery
 */
public class ProcessImageResponseDoFn
    extends DoFn<KV<String, AnnotateImageResponse>, KV<BQDestination, TableRow>> {
  private static final long serialVersionUID = 1L;
  public static final Logger LOG = LoggerFactory.getLogger(AnnotateImagesDoFn.class);

  private final Counter numberOfCropHintsAnnotations =
      Metrics.counter(ProcessImageResponseDoFn.class, "numberOfCropHintsAnnotations");
  private final Counter numberOfImageProperties =
      Metrics.counter(ProcessImageResponseDoFn.class, "numberOfImageProperties");

  private final Collection<AnnotationProcessor> processors;
  public ProcessImageResponseDoFn(Collection<AnnotationProcessor> processors) {
    this.processors = processors;
  }

  @ProcessElement
  public void processElement(@Element KV<String, AnnotateImageResponse> element,
      OutputReceiver<KV<BQDestination,TableRow>> out) {
    String imageFileURI = element.getKey();
    AnnotateImageResponse annotationResponse = element.getValue();

    VisionAnalyticsPipeline.processedFiles.inc();

    processors.forEach(processor -> {
      Iterable<KV<BQDestination, TableRow>> processingResult = processor.extractAnnotations(imageFileURI, annotationResponse);
      if(processingResult != null) {
        processingResult.forEach(outcome -> out.output(outcome));
      }
    });
//
//    annotationResponse
//        .getAllFields()
//        .entrySet()
//        .forEach(
//            response -> {
//              String key = response.getKey().getJsonName();
//              try {
//                switch (key) {
//                  case "cropHintsAnnotation":
//                    if (annotationResponse.hasCropHintsAnnotation()) {
//                      CropHintsAnnotation annotation = annotationResponse.getCropHintsAnnotation();
//                      for (CropHint crophint : annotation.getCropHintsList()) {
//                        numberOfCropHintsAnnotations.inc();
//                        Row row = Util.transformCropHintsAnnotations(imageFileURI, crophint);
//                        out.output(
//                            KV.of(
//                                Util.BQ_TABLE_NAME_MAP.get(
//                                    "BQ_TABLE_NAME_CROP_HINTS_ANNOTATION"),
//                                Util.toTableRow(row)));
//                      }
//                    }
//                    break;
//
//                  case "imagePropertiesAnnotation":
//                    if (annotationResponse.hasImagePropertiesAnnotation()) {
//                      numberOfImageProperties.inc();
//                      Row row =
//                          Util.transformImagePropertiesAnnotations(
//                              imageFileURI, annotationResponse.getImagePropertiesAnnotation());
//                      out
//                          .output(
//                              KV.of(
//                                  Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_IMAGE_PROP_ANNOTATION"),
//                                  Util.toTableRow(row)));
//                    }
//                    if (annotationResponse.hasCropHintsAnnotation()) {
//                      CropHintsAnnotation annotation = annotationResponse.getCropHintsAnnotation();
//                      for (CropHint crophint : annotation.getCropHintsList()) {
//                        numberOfCropHintsAnnotations.inc();
//
//                        Row row = Util.transformCropHintsAnnotations(imageFileURI, crophint);
//                        out.output(
//                            KV.of(
//                                Util.BQ_TABLE_NAME_MAP.get(
//                                    "BQ_TABLE_NAME_CROP_HINTS_ANNOTATION"),
//                                Util.toTableRow(row)));
//                      }
//                    }
//                    break;   }
//              } catch (Exception e) {
//                LOG.error(
//                    "Error '{}' processing response for file '{}'", e.getMessage(), imageFileURI);
//                out.output(
//                    KV.of(
//                        Util.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
//                        Util.toTableRow(
//                            Row.withSchema(Util.errorSchema)
//                                .addValues(
//                                    imageFileURI,
//                                    Util.getTimeStamp(),
//                                    e.getMessage(),
//                                    ExceptionUtils.getStackTrace(e))
//                                .build())));
//              }
//            });
  }
}
