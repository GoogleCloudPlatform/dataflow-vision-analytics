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
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.cloud.vision.v1.LocalizedObjectAnnotation;
import com.google.cloud.vision.v1.ProductSearchResults;
import com.google.cloud.vision.v1.SafeSearchAnnotation;
import com.google.cloud.vision.v1.WebDetection;
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
  public void processElement(ProcessContext c) {
    String imageName = c.element().getKey();
    AnnotateImageResponse imageResponse = c.element().getValue();
    imageResponse
        .getAllFields()
        .entrySet()
        .forEach(
            element -> {
              String key = element.getKey().getJsonName();
              try {
                switch (key) {
                  case "labelAnnotations":
                    numberOfAnnotationResponse.inc(imageResponse.getLabelAnnotationsCount());
                    for (EntityAnnotation annotation : imageResponse.getLabelAnnotationsList()) {

                      Row row = Util.transformLabelAnnotations(imageName, annotation);
                      c.output(
                          KV.of(
                              Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LABEL_ANNOTATION"),
                              Util.toTableRow(row)));
                    }
                    break;
                  case "landmarkAnnotations":
                    numberOfAnnotationResponse.inc(imageResponse.getLandmarkAnnotationsCount());
                    for (EntityAnnotation annotation : imageResponse.getLandmarkAnnotationsList()) {
                      Row row = Util.transformLandmarkAnnotations(imageName, annotation);
                      c.output(
                          KV.of(
                              Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LANDMARK_ANNOTATION"),
                              Util.toTableRow(row)));
                    }
                    break;
                  case "logoAnnotations":
                    numberOfAnnotationResponse.inc(imageResponse.getLogoAnnotationsCount());
                    for (EntityAnnotation annotation : imageResponse.getLogoAnnotationsList()) {
                      //                      Row row =
                      //                          Util.convertEntityAnnotationProtoToJson(
                      //                              imageName,
                      //                              annotation,
                      //
                      // Feature.newBuilder().setType(Type.LOGO_DETECTION).build());
                      //                      c.output(
                      //                          KV.of(
                      //
                      // Util.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_ENTITY_ANNOTATION"),
                      //                              Util.toTableRow(row)));
                    }
                    break;
                  case "faceAnnotations":
                    numberOfAnnotationResponse.inc(imageResponse.getFaceAnnotationsCount());

                    for (FaceAnnotation annotation : imageResponse.getFaceAnnotationsList()) {
                      //                    		 GenericJson json
                      // =VisionApiUtil.convertFaceAnnotationProtoToJson(annotation);
                      //
                      // c.output(VisionApiUtil.labelAnnotationTag,KV.of(
                      //
                      // VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_FACE_ANNOTATION"),json));

                    }
                    break;
                  case "cropHintsAnnotation":
                    if (imageResponse.hasCropHintsAnnotation()) {
                      //                        CropHintsAnnotation annotation =
                      // imageResponse.getCropHintsAnnotation();
                      //                        numberOfAnnotationResponse.inc(1);
                      //                        GenericJson json
                      // =VisionApiUtil.convertCorpHintAnnotationProtoToJson(annotation);
                      //                        c.output(VisionApiUtil.labelAnnotationTag,KV.of(
                      //
                      // VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_CORP_HINTS_ANNOTATION"),json));

                    }
                    break;
                  case "fullTextAnnotation":
                    if (imageResponse.hasFullTextAnnotation()) {
                      //                        TextAnnotation annotation =
                      // imageResponse.getFullTextAnnotation();
                      //                        numberOfAnnotationResponse.inc(1);
                      //                        GenericJson json
                      // =VisionApiUtil.convertFullTextAnnotationProtoToJson(annotation);
                      //                        c.output(VisionApiUtil.labelAnnotationTag,KV.of(
                      //
                      // VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_FULL_TEXT_ANNOTATION"),json));

                    }
                    break;
                  case "imagePropertiesAnnotation":
                    if (imageResponse.hasImagePropertiesAnnotation()) {
                      //                        ImageProperties annotation =
                      // imageResponse.getImagePropertiesAnnotation();
                      //                        numberOfAnnotationResponse.inc(1);
                      //                        GenericJson json
                      // =VisionApiUtil.convertImagePropertiesAnnotationProtoToJson(annotation);
                      //                        c.output(VisionApiUtil.labelAnnotationTag,KV.of(
                      //
                      // VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_IMAGE_PROP_ANNOTATION"),json));

                    }
                    break;
                  case "localizedObjectAnnotations":
                    numberOfAnnotationResponse.inc(
                        imageResponse.getLocalizedObjectAnnotationsCount());
                    for (LocalizedObjectAnnotation annotation :
                        imageResponse.getLocalizedObjectAnnotationsList()) {
                      //                    		GenericJson json
                      // =VisionApiUtil.convertLocalizedObjectAnnotationProtoToJson(annotation);
                      //                            c.output(VisionApiUtil.labelAnnotationTag,KV.of(
                      //
                      // VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LOCALIZED_OBJECT_ANNOTATION"),json));

                    }
                    break;
                  case "productSearchResults":
                    if (imageResponse.hasProductSearchResults()) {
                      ProductSearchResults annotation = imageResponse.getProductSearchResults();
                      numberOfAnnotationResponse.inc(1);
                      //                        GenericJson json
                      // =VisionApiUtil.convertProductSearchAnnotationProtoToJson(annotation);
                      //                        c.output(VisionApiUtil.labelAnnotationTag,KV.of(
                      //
                      // VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_PRODUCT_SEARCH_RESULT"),json));

                    }
                    break;
                  case "safeSearchAnnotation":
                    if (imageResponse.hasSafeSearchAnnotation()) {
                      SafeSearchAnnotation annotation = imageResponse.getSafeSearchAnnotation();
                      numberOfAnnotationResponse.inc(1);
                      //                        GenericJson json
                      // =VisionApiUtil.convertSafeAnnotationProtoToJson(annotation);
                      //                        c.output(VisionApiUtil.labelAnnotationTag,KV.of(
                      //
                      // VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_SAFE_SEARCH_ANNOTATION"),json));

                    }
                    break;
                  case "textAnnotations":
                    numberOfAnnotationResponse.inc(imageResponse.getTextAnnotationsCount());
                    for (EntityAnnotation annotation : imageResponse.getTextAnnotationsList()) {
                      //                    		 GenericJson json
                      // =VisionApiUtil.convertEntityAnnotationProtoToJson(annotation);
                      //
                      // c.output(VisionApiUtil.labelAnnotationTag,KV.of(
                      //
                      // VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_TEXT_ANNOTATION"),json));

                    }
                    break;
                  case "webDetection":
                    if (imageResponse.hasWebDetection()) {
                      WebDetection annotation = imageResponse.getWebDetection();
                      numberOfAnnotationResponse.inc(1);
                      //                        GenericJson json
                      // =VisionApiUtil.convertWebDetectionProtoToJson(annotation);
                      //                        c.output(VisionApiUtil.labelAnnotationTag,KV.of(
                      //
                      // VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_WEB_DETECTION_ANNOTATION"),json));

                    }
                    break;
                  default:
                    ErrorMessageBuilder errorBuilder =
                        ErrorMessageBuilder.newBuilder()
                            .setErrorMessage("No Feature Type Found")
                            .setFileName(imageName)
                            .setTimeStamp(Util.getTimeStamp())
                            .build()
                            .withTableRow(new TableRow());
                }
              } catch (Exception e) {
                LOG.error("Select Column mode exception {}", e.getMessage());
                e.printStackTrace();
              }
            });
  }
}
