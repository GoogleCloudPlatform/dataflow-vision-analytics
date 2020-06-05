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

import com.google.api.client.json.GenericJson;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.CropHintsAnnotation;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.cloud.vision.v1.ImageProperties;
import com.google.cloud.vision.v1.LocalizedObjectAnnotation;
import com.google.cloud.vision.v1.ProductSearchResults;
import com.google.cloud.vision.v1.SafeSearchAnnotation;
import com.google.cloud.vision.v1.TextAnnotation;
import com.google.cloud.vision.v1.WebDetection;
import com.google.protobuf.FieldMask;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessImageResponse {@link ProcessImageResponse} class parses the image response for specific
 * annotation and using image response builder output the table and table row for BigQuery
 */
public class ProcessImageResponse
    extends DoFn<KV<String, AnnotateImageResponse>, KV<String, TableRow>> {
  public static final Logger LOG = LoggerFactory.getLogger(CreateImageReqest.class);

  private boolean rawJsonMode;
  private String bqTableName;
  PCollectionView<Map<String, FieldMask>> selectedColumnAnnotationMap;

  public ProcessImageResponse(
      boolean rawJsonMode, PCollectionView<Map<String, FieldMask>> selectedColumnAnnotationMap) {
    this.selectedColumnAnnotationMap = selectedColumnAnnotationMap;
    this.rawJsonMode = rawJsonMode;
    this.bqTableName = VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_DEFAULT_MODE");
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String imageName = c.element().getKey();
    AnnotateImageResponse imageResponse = c.element().getValue();
    if (!rawJsonMode) {

      imageResponse
          .getAllFields()
          .entrySet()
          .forEach(
              element -> {
                String key = element.getKey().getJsonName();
                String annotationElement =
                    c.sideInput(selectedColumnAnnotationMap).containsKey(key) ? key : null;
                try {
                  switch (annotationElement) {
                    case "labelAnnotations":
                      for (EntityAnnotation annotation : imageResponse.getLabelAnnotationsList()) {
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LABEL_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withEntityAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "landmarkAnnotations":
                      for (EntityAnnotation annotation :
                          imageResponse.getLandmarkAnnotationsList()) {
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get(
                                "BQ_TABLE_NAME_LANDMARK_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withEntityAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "logoAnnotations":
                      for (EntityAnnotation annotation : imageResponse.getLogoAnnotationsList()) {
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_LOGO_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withEntityAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "faceAnnotations":
                      for (FaceAnnotation annotation : imageResponse.getFaceAnnotationsList()) {
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_FACE_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withFaceAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "cropHintsAnnotation":
                      if (imageResponse.hasCropHintsAnnotation()) {
                        CropHintsAnnotation annotation = imageResponse.getCropHintsAnnotation();
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get(
                                "BQ_TABLE_NAME_CORP_HINTS_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withCropHintsAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "fullTextAnnotation":
                      if (imageResponse.hasFullTextAnnotation()) {
                        TextAnnotation annotation = imageResponse.getFullTextAnnotation();
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get(
                                "BQ_TABLE_NAME_FULL_TEXT_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withTextAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "imagePropertiesAnnotation":
                      if (imageResponse.hasImagePropertiesAnnotation()) {
                        ImageProperties annotation = imageResponse.getImagePropertiesAnnotation();
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get(
                                "BQ_TABLE_NAME_IMAGE_PROP_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withImagePropAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "localizedObjectAnnotations":
                      for (LocalizedObjectAnnotation annotation :
                          imageResponse.getLocalizedObjectAnnotationsList()) {
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get(
                                "BQ_TABLE_NAME_LOCALIZED_OBJECT_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withLocalizedObjAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "productSearchResults":
                      if (imageResponse.hasProductSearchResults()) {
                        ProductSearchResults annotation = imageResponse.getProductSearchResults();
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get(
                                "BQ_TABLE_NAME_PRODUCT_SEARCH_RESULT");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withProductSchResultAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "safeSearchAnnotation":
                      if (imageResponse.hasSafeSearchAnnotation()) {
                        SafeSearchAnnotation annotation = imageResponse.getSafeSearchAnnotation();
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get(
                                "BQ_TABLE_NAME_SAFE_SEARCH_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withSafeSchResultAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "textAnnotations":
                      for (EntityAnnotation annotation : imageResponse.getTextAnnotationsList()) {
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_TABLE_NAME_TEXT_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withEntityAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    case "webDetection":
                      if (imageResponse.hasWebDetection()) {
                        WebDetection annotation = imageResponse.getWebDetection();
                        FieldMask mask = c.sideInput(selectedColumnAnnotationMap).get(key);
                        bqTableName =
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get(
                                "BQ_TABLE_NAME_WEB_DETECTION_ANNOTATION");
                        ImageResponseBuilder response =
                            ImageResponseBuilder.newBuilder()
                                .setFieldMask(mask)
                                .setImageName(imageName)
                                .setTableRow(new TableRow())
                                .build()
                                .withWebDetectionAnnotation(annotation);
                        c.output(KV.of(bqTableName, response.tableRow()));
                      }
                      break;
                    default:
                      ErrorMessageBuilder errorBuilder =
                          ErrorMessageBuilder.newBuilder()
                              .setErrorMessage("No Feature Type Found")
                              .setFileName(imageName)
                              .setTimeStamp(VisionApiUtil.getTimeStamp())
                              .build()
                              .withTableRow(new TableRow());
                      c.output(
                          VisionApiUtil.failureTag,
                          KV.of(
                              VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
                              errorBuilder.tableRow()));
                  }
                } catch (Exception e) {
                  LOG.error("Select Column mode exception {}", e.getMessage());
                  ErrorMessageBuilder errorBuilder =
                      ErrorMessageBuilder.newBuilder()
                          .setErrorMessage(e.getMessage())
                          .setStackTrace(e.getStackTrace().toString())
                          .setFileName(imageName)
                          .setTimeStamp(VisionApiUtil.getTimeStamp())
                          .build()
                          .withTableRow(new TableRow());
                  c.output(
                      VisionApiUtil.failureTag,
                      KV.of(
                          VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
                          errorBuilder.tableRow()));
                }
              });

    } else {
      try {
        GenericJson json = VisionApiUtil.convertImageAnnotationToJson(imageResponse);
        json.entrySet()
            .forEach(
                entry -> {
                  try {
                    GenericJson modifiedJson =
                        VisionApiUtil.applyRawJsonFormat(
                            imageName, entry.getKey(), entry.getValue());
                    c.output(KV.of(bqTableName, VisionApiUtil.convertJsonToTableRow(modifiedJson)));
                  } catch (IOException e) {
                    ErrorMessageBuilder errorBuilder =
                        ErrorMessageBuilder.newBuilder()
                            .setErrorMessage(e.getMessage())
                            .setStackTrace(e.getStackTrace().toString())
                            .setFileName(imageName)
                            .setTimeStamp(VisionApiUtil.getTimeStamp())
                            .build()
                            .withTableRow(new TableRow());
                    c.output(
                        VisionApiUtil.failureTag,
                        KV.of(
                            VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
                            errorBuilder.tableRow()));
                  }
                });
      } catch (IOException e) {
        ErrorMessageBuilder errorBuilder =
            ErrorMessageBuilder.newBuilder()
                .setErrorMessage(e.getMessage())
                .setStackTrace(e.getStackTrace().toString())
                .setFileName(imageName)
                .setTimeStamp(VisionApiUtil.getTimeStamp())
                .build()
                .withTableRow(new TableRow());
        c.output(
            VisionApiUtil.failureTag,
            KV.of(VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"), errorBuilder.tableRow()));
      }
    }
  }
}
