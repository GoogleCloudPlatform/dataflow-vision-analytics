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
import com.google.api.services.bigquery.model.TableRow;
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
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util class to reuse some convert methods used in the pipeline multiple times */
public class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);
  private static final DateTimeFormatter BIGQUERY_TIMESTAMP_PRINTER;

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
          .put("BQ_TABLE_NAME_ENTITY_ANNOTATION", "ENTITY_ANNOTATION")
          .put("BQ_TABLE_NAME_LABEL_ANNOTATION", "LABEL_ANNOTATION")
          .put("BQ_TABLE_NAME_LANDMARK_ANNOTATION", "LANDMARK_ANNOTATION")
          .put("BQ_TABLE_NAME_FACE_ANNOTATION", "FACE_DETECTION")
          .put("BQ_TABLE_NAME_CORP_HINTS_ANNOTATION", "CORP_HINTS_ANNOTATION")
          .put("BQ_TABLE_NAME_FULL_TEXT_ANNOTATION", "TEXT_ANNOTATION")
          .put("BQ_TABLE_NAME_IMAGE_PROP_ANNOTATION", "IMAGE_PROPERTIES")
          .put("BQ_TABLE_NAME_LOCALIZED_OBJECT_ANNOTATION", "LOCALIZED_OBJECT_ANNOTATION")
          .put("BQ_TABLE_NAME_PRODUCT_SEARCH_RESULT", "PRODUCT_SEARCH_RESULT")
          .put("BQ_TABLE_NAME_SAFE_SEARCH_ANNOTATION", "SAFE_SEARCH_ANNOTATION")
          .put("BQ_TABLE_NAME_WEB_DETECTION_ANNOTATION", "WEB_DETECTION")
          .build();

  static {
    DateTimeFormatter dateTimePart =
        new DateTimeFormatterBuilder()
            .appendYear(4, 4)
            .appendLiteral('-')
            .appendMonthOfYear(2)
            .appendLiteral('-')
            .appendDayOfMonth(2)
            .appendLiteral(' ')
            .appendHourOfDay(2)
            .appendLiteral(':')
            .appendMinuteOfHour(2)
            .appendLiteral(':')
            .appendSecondOfMinute(2)
            .toFormatter()
            .withZoneUTC();
    BIGQUERY_TIMESTAMP_PRINTER =
        new DateTimeFormatterBuilder()
            .append(dateTimePart)
            .appendLiteral('.')
            .appendFractionOfSecond(3, 3)
            .appendLiteral(" UTC")
            .toFormatter();
  }

  public static Feature convertJsonToFeature(String json) throws InvalidProtocolBufferException {

    Feature.Builder feature = Feature.newBuilder();
    TypeRegistry registry = TypeRegistry.newBuilder().add(feature.getDescriptorForType()).build();
    JsonFormat.Parser jFormatter = JsonFormat.parser().usingTypeRegistry(registry);
    if (jFormatter != null) {
      jFormatter.merge(json.toString(), feature);
    }

    return feature.build();
  }

  public static final Schema verticSchema =
      Stream.of(
              Schema.Field.of("x", FieldType.INT32).withNullable(true),
              Schema.Field.of("y", FieldType.INT32).withNullable(true))
          .collect(toSchema());

  //  public static final Schema verticesSchema =
  //      Stream.of(
  //              Schema.Field.of("vertic", FieldType.array(FieldType.row(verticSchema)))
  //                  .withNullable(true))
  //          .collect(toSchema());

  public static final Schema boundingPolySchema =
      Stream.of(
              Schema.Field.of("vertices", FieldType.array(FieldType.row(verticSchema)))
                  .withNullable(true))
          .collect(toSchema());

  public static final Schema latLonSchema =
      Stream.of(
              Schema.Field.of("latitude", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("longitude", FieldType.DOUBLE).withNullable(true))
          .collect(toSchema());

  //  public static final Schema locationsSchema =
  //		  Stream.of(
  //	              Schema.Field.of("latLon", FieldType.array(FieldType.row(latLonSchema)))
  //	                  .withNullable(true))
  //	          .collect(toSchema());
  public static final Schema locationSchema =
      Stream.of(Schema.Field.of("latLon", FieldType.row(latLonSchema)).withNullable(true))
          .collect(toSchema());

  public static final Schema labelAnnotationSchema =
      Stream.of(
              Schema.Field.of("gcsUri", FieldType.STRING).withNullable(true),
              Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
              Schema.Field.of("mid", FieldType.STRING).withNullable(true),
              Schema.Field.of("description", FieldType.STRING).withNullable(true),
              Schema.Field.of("score", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("topicality", FieldType.FLOAT).withNullable(true))
          .collect(toSchema());
  public static final Schema landmarkAnnotationSchema =
      Stream.of(
              Schema.Field.of("gcsUri", FieldType.STRING).withNullable(true),
              Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
              Schema.Field.of("mid", FieldType.STRING).withNullable(true),
              Schema.Field.of("description", FieldType.STRING).withNullable(true),
              Schema.Field.of("score", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("boundingPoly", FieldType.row(boundingPolySchema)).withNullable(true),
              Schema.Field.of("locations", FieldType.array(FieldType.row(locationSchema)))
                  .withNullable(true))
          .collect(toSchema());

  public static Row transformLabelAnnotations(String imageName, EntityAnnotation annotation) {
    Row row =
        Row.withSchema(labelAnnotationSchema)
            .addValues(
                imageName,
                "label_annotation",
                annotation.getMid(),
                annotation.getDescription(),
                annotation.getScore(),
                annotation.getTopicality())
            .build();
    LOG.info("Row {}", row.toString());
    return row;
  }

  public static Row transformLandmarkAnnotations(String imageName, EntityAnnotation annotation) {
    List<Row> verticesList = new ArrayList<>();
    List<Row> locationList = new ArrayList<>();

    if (annotation.hasBoundingPoly()) {

      annotation
          .getBoundingPoly()
          .getVerticesList()
          .forEach(
              vertics -> {
                verticesList.add(
                    Row.withSchema(verticSchema).addValues(vertics.getX(), vertics.getY()).build());
              });
    }

    if (annotation.getLocationsCount() > 0) {
      annotation
          .getLocationsList()
          .forEach(
              location -> {
                double latitude = location.getLatLng().getLatitude();
                double longitude = location.getLatLng().getLongitude();
                locationList.add(
                    Row.withSchema(latLonSchema).addValues(latitude, longitude).build());
              });
    }

    Row row =
        Row.withSchema(landmarkAnnotationSchema)
            .addValues(
                imageName,
                "landmark_annotation",
                annotation.getMid(),
                annotation.getDescription(),
                annotation.getScore(),
                verticesList,
                Row.withSchema(locationSchema).addArray(locationList).build()).build();

          

    LOG.info("Row {}", row.toString());
    return row;
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

  private static Object fromBeamField(FieldType fieldType, Object fieldValue) {
    if (fieldValue == null) {
      if (!fieldType.getNullable()) {
        throw new IllegalArgumentException("Field is not nullable.");
      }
      return null;
    }

    switch (fieldType.getTypeName()) {
      case ARRAY:
      case ITERABLE:
        FieldType elementType = fieldType.getCollectionElementType();
        Iterable items = (Iterable) fieldValue;
        List convertedItems = Lists.newArrayListWithCapacity(Iterables.size(items));
        for (Object item : items) {
          convertedItems.add(fromBeamField(elementType, item));
        }
        return convertedItems;

      case ROW:
        return toTableRow((Row) fieldValue);

      case DATETIME:
        return ((Instant) fieldValue)
            .toDateTime(DateTimeZone.UTC)
            .toString(BIGQUERY_TIMESTAMP_PRINTER);

      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN:
        return fieldValue.toString();

      case DECIMAL:
        return fieldValue.toString();

      case BYTES:
        return BaseEncoding.base64().encode((byte[]) fieldValue);

      default:
        return fieldValue;
    }
  }

  public static TableRow toTableRow(Row row) {
    TableRow output = new TableRow();
    for (int i = 0; i < row.getFieldCount(); i++) {
      Object value = row.getValue(i);
      Field schemaField = row.getSchema().getField(i);
      output = output.set(schemaField.getName(), fromBeamField(schemaField.getType(), value));
    }
    return output;
  }
}
