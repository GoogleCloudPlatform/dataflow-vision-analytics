/*
 * Copyright (C) 2019 Google Inc.
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

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.google.api.client.json.GenericJson;
import com.google.cloud.vision.v1.CropHintsAnnotation;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.ImageProperties;
import com.google.cloud.vision.v1.LocalizedObjectAnnotation;
import com.google.cloud.vision.v1.ProductSearchResults;
import com.google.cloud.vision.v1.SafeSearchAnnotation;
import com.google.cloud.vision.v1.TextAnnotation;
import com.google.cloud.vision.v1.WebDetection;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.FieldMask;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util class to reuse some convert methods used in the pipeline multiple times */
public class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);

  public static final String ALLOWED_NOTIFICATION_EVENT_TYPE = String.valueOf("OBJECT_FINALIZE");
  /** Allowed image extension supported by Vision API */
  public static final String FILE_PATTERN = "(^.*\\.(JPEG|jpeg|JPG|jpg|PNG|png|GIF|gif)$)";
  /** Error message if no valid extension found */
  public static final String NO_VALID_EXT_FOUND_ERROR_MESSAGE =
      "File {} does not contain a valid extension";

  /** Default interval for polling files in GCS. */
  public static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(5);

  public static Gson gson = new Gson();
  /** Process time stamp field added part of BigQuery Column */
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  public static final Map<String, String> BQ_TABLE_NAME_MAP =
      ImmutableMap.<String, String>builder()
          .put("BQ_TABLE_NAME_DEFAULT_MODE", "VISION_API_FINDINGS_RAW_JSON")
          .put("BQ_TABLE_NAME_LABEL_ANNOTATION", "LABEL_DETECTION")
          .put("BQ_TABLE_NAME_FACE_ANNOTATION", "VISION_API_FINDINGS_FACE_DETECTION")
          .put("BQ_TABLE_NAME_LANDMARK_ANNOTATION", "VISION_API_FINDINGS_LANDMARK_DETECTION")
          .put("BQ_TABLE_NAME_LOGO_ANNOTATION", "VISION_API_FINDINGS_LOGO_DETECTION")
          .put("BQ_TABLE_NAME_CORP_HINTS_ANNOTATION", "VISION_API_FINDINGS_CORP_HINTS_DETECTION")
          .put("BQ_TABLE_NAME_FULL_TEXT_ANNOTATION", "VISION_API_FINDINGS_FULL_TEXT_DETECTION")
          .put("BQ_TABLE_NAME_IMAGE_PROP_ANNOTATION", "VISION_API_FINDINGS_IMAGE_PROP_DETECTION")
          .put(
              "BQ_TABLE_NAME_LOCALIZED_OBJECT_ANNOTATION",
              "VISION_API_FINDINGS_LOCALIZED_OBJECT_DETECTION")
          .put("BQ_TABLE_NAME_PRODUCT_SEARCH_RESULT", "VISION_API_FINDINGS_PRODUCT_SEARCH_RESULT")
          .put("BQ_TABLE_NAME_SAFE_SEARCH_ANNOTATION", "VISION_API_FINDINGS_SAFE_SEARCH_RESULT")
          .put("BQ_TABLE_NAME_TEXT_ANNOTATION", "VISION_API_FINDINGS_TEXT_DETECTION")
          .put("BQ_TABLE_NAME_WEB_DETECTION_ANNOTATION", "VISION_API_FINDINGS_WEB_DETECTION")
          .put("BQ_ERROR_TABLE", "VISION_API_ERROR_TABLE")
          .build();

  public static Feature convertJsonToFeature(String json) throws InvalidProtocolBufferException {

    Feature.Builder feature = Feature.newBuilder();
    TypeRegistry registry = TypeRegistry.newBuilder().add(feature.getDescriptorForType()).build();
    JsonFormat.Parser jFormatter = JsonFormat.parser().usingTypeRegistry(registry);
    if (jFormatter != null) {
      jFormatter.merge(json.toString(), feature);
    }

    return feature.build();
  }

  public static Row convertEntityAnnotationProtoToJson(EntityAnnotation annotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    List<Row> verticesList = new ArrayList<>();
    List<Row> normalizedVerticesList = new ArrayList<>();
    List<Row> locationList = new ArrayList<>();
    List<Row> propertyList = new ArrayList<>();
    if (annotation.hasBoundingPoly()) {
      annotation
          .getBoundingPoly()
          .getVerticesList()
          .forEach(
              vertics -> {
                verticesList.add(
                    Row.withSchema(vertic).addValues(vertics.getX(), vertics.getY()).build());
              });
      annotation
          .getBoundingPoly()
          .getNormalizedVerticesList()
          .forEach(
              vertics -> {
                normalizedVerticesList.add(
                    Row.withSchema(vertic).addValues(vertics.getX(), vertics.getY()).build());
              });
    }
    if (annotation.getLocationsCount() > 0) {
      annotation
          .getLocationsList()
          .forEach(
              location -> {
                double latitude = location.getLatLng().getLatitude();
                double longitude = location.getLatLng().getLongitude();
                locationList.add(Row.withSchema(latLon).addValues(latitude, longitude).build());
              });
    }

    if (annotation.getPropertiesCount() > 0) {
      annotation
          .getPropertiesList()
          .forEach(
              prop -> {
                propertyList.add(
                    Row.withSchema(property)
                        .addValues(prop.getName(), prop.getValue(), prop.getUint64Value())
                        .build());
              });
    }

    return Row.withSchema(entityAnnotation)
        .addValues(
            annotation.getMid(),
            annotation.getDescription(),
            annotation.getScore(),
            annotation.getTopicality(),
            Row.withSchema(boundingPoly).addValues(verticesList, normalizedVerticesList),
            Row.withSchema(latLon).addArray(locationList),
            Row.withSchema(property).addArray(propertyList))
        .build();
  }

  public static GenericJson convertFaceAnnotationProtoToJson(FaceAnnotation annotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(annotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson convertCorpHintAnnotationProtoToJson(CropHintsAnnotation annotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(annotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson convertTextAnnotationProtoToJson(TextAnnotation annotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(annotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson convertImagePropertiesAnnotationProtoToJson(ImageProperties annotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(annotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson convertLocalizedObjectAnnotationProtoToJson(
      LocalizedObjectAnnotation annotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(annotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson convertProductSearchAnnotationProtoToJson(
      ProductSearchResults annotation) throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(annotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson convertSafeAnnotationProtoToJson(SafeSearchAnnotation annotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(annotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson convertWebDetectionProtoToJson(WebDetection annotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(annotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson convertFullTextAnnotationProtoToJson(TextAnnotation annotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(annotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static String getTimeStamp() {
    return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));
  }

  private static FieldMask convertStringToFieldMask(String selectedCloumns) {
    Iterable<String> paths = Arrays.asList(selectedCloumns.split(","));
    FieldMask.Builder fieldMaskBuilder = FieldMask.newBuilder();
    for (String path : paths) {
      if (path.isEmpty()) {
        continue;
      }
      fieldMaskBuilder.addPaths(path);
    }
    FieldMask masks = fieldMaskBuilder.build();
    LOG.debug("Field Mask Config {}", masks.toString());
    return fieldMaskBuilder.build();
  }

  public static Map<String, FieldMask> convertJsonToFieldMask(String json) throws Exception {

    Map<String, FieldMask> dataMap = new HashMap<String, FieldMask>();

    if (json != null) {
      JsonObject jsonConfig = gson.fromJson(json, new TypeToken<JsonObject>() {}.getType());
      jsonConfig
          .get("selectedColumns")
          .getAsJsonArray()
          .forEach(
              element -> {
                element
                    .getAsJsonObject()
                    .entrySet()
                    .forEach(
                        entry -> {
                          dataMap.put(
                              entry.getKey(),
                              convertStringToFieldMask(entry.getValue().getAsString()));
                        });
              });
    }
    return dataMap;
  }

  public static final Schema vertic =
      Stream.of(
              Schema.Field.of("x", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("y", FieldType.DOUBLE).withNullable(true))
          .collect(toSchema());

  public static final Schema normalizedVertic =
      Stream.of(
              Schema.Field.of("x", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("y", FieldType.DOUBLE).withNullable(true))
          .collect(toSchema());

  public static final Schema boundingPoly =
      Stream.of(
              Schema.Field.of("vertices", FieldType.array(FieldType.row(vertic)))
                  .withNullable(true),
              Schema.Field.of(
                      "normalizedVertices", FieldType.array(FieldType.row(normalizedVertic)))
                  .withNullable(true))
          .collect(toSchema());

  public static final Schema latLon =
      Stream.of(
              Schema.Field.of("latitude", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("longitude", FieldType.DOUBLE).withNullable(true))
          .collect(toSchema());

  public static final Schema property =
      Stream.of(
              Schema.Field.of("name", FieldType.STRING).withNullable(true),
              Schema.Field.of("value", FieldType.STRING).withNullable(true),
              Schema.Field.of("unit64_value", FieldType.INT64).withNullable(true))
          .collect(toSchema());
  public static final Schema entityAnnotation =
      Stream.of(
              Schema.Field.of("mid", FieldType.STRING).withNullable(true),
              Schema.Field.of("description", FieldType.STRING).withNullable(true),
              Schema.Field.of("score", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("topicality", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("bounding_poly", FieldType.row(boundingPoly)).withNullable(true),
              Schema.Field.of("locations", FieldType.array(FieldType.row(latLon)))
                  .withNullable(true),
              Schema.Field.of("properties", FieldType.array(FieldType.row(property)))
                  .withNullable(true))
          .collect(toSchema());
}
