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

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.CropHint;
import com.google.cloud.vision.v1.DominantColorsAnnotation;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.cloud.vision.v1.ImageProperties;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util class to reuse some convert methods used in the pipeline multiple times */
@SuppressWarnings("serial")
public class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);
  private static final DateTimeFormatter BIGQUERY_TIMESTAMP_PRINTER;
  public static final TupleTag<KV<String, TableRow>> apiResponseSuccessElements =
      new TupleTag<KV<String, TableRow>>() {};
  public static final TupleTag<KV<String, TableRow>> apiResponseFailedElements =
      new TupleTag<KV<String, TableRow>>() {};
  public static final String ALLOWED_NOTIFICATION_EVENT_TYPE = String.valueOf("OBJECT_FINALIZE");
  /** Allowed image extension supported by Vision API */
  public static final String FILE_PATTERN = "(^.*\\.(JPEG|jpeg|JPG|jpg|PNG|png|GIF|gif)$)";
  /** Error message if no valid extension found */
  public static final String NO_VALID_EXT_FOUND_ERROR_MESSAGE =
      "File {} does not contain a valid extension";

  public static final String FEATURE_TYPE_NOT_SUPPORTED = "Feature Type {} is not supported";

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  public static final Map<String, String> BQ_TABLE_NAME_MAP =
      ImmutableMap.<String, String>builder()
          .put("BQ_TABLE_NAME_LOGO_ANNOTATION", "LOGO_ANNOTATION")
          .put("BQ_TABLE_NAME_LABEL_ANNOTATION", "LABEL_ANNOTATION")
          .put("BQ_TABLE_NAME_LANDMARK_ANNOTATION", "LANDMARK_ANNOTATION")
          .put("BQ_TABLE_NAME_FACE_ANNOTATION", "FACE_ANNOTATION")
          .put("BQ_TABLE_NAME_CROP_HINTS_ANNOTATION", "CROP_HINTS_ANNOTATION")
          .put("BQ_TABLE_NAME_IMAGE_PROP_ANNOTATION", "IMAGE_PROPERTIES")
          .put("BQ_ERROR_TABLE", "ERROR_LOG")
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

  public static final Schema errorSchema =
      Stream.of(
              Schema.Field.of("file_name", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
              Schema.Field.of("error_messagee", FieldType.STRING).withNullable(true),
              Schema.Field.of("stack_trace", FieldType.STRING).withNullable(true))
          .collect(toSchema());

  public static final Schema verticSchema =
      Stream.of(
              Schema.Field.of("x", FieldType.INT32).withNullable(true),
              Schema.Field.of("y", FieldType.INT32).withNullable(true))
          .collect(toSchema());
  public static final Schema colorSchema =
      Stream.of(
              Schema.Field.of("red", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("green", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("blue", FieldType.FLOAT).withNullable(true))
          .collect(toSchema());
  public static final Schema colorInfoSchema =
      Stream.of(
              Schema.Field.of("score", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("pixelFraction", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("color", FieldType.row(colorSchema)).withNullable(true))
          .collect(toSchema());
  public static final Schema colorListSchema =
      Stream.of(
              Schema.Field.of("colors", FieldType.array(FieldType.row(colorInfoSchema)))
                  .withNullable(true))
          .collect(toSchema());

  public static final Schema positionSchema =
      Stream.of(
              Schema.Field.of("x", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("y", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("z", FieldType.FLOAT).withNullable(true))
          .collect(toSchema());
  public static final Schema landmarkSchema =
      Stream.of(
              Schema.Field.of("type", FieldType.STRING).withNullable(true),
              Schema.Field.of("position", FieldType.row(positionSchema)).withNullable(true))
          .collect(toSchema());

  public static final Schema boundingPolySchema =
      Stream.of(
              Schema.Field.of("vertices", FieldType.array(FieldType.row(verticSchema)))
                  .withNullable(true))
          .collect(toSchema());
  public static final Schema fdboundingPolySchema =
      Stream.of(
              Schema.Field.of("vertices", FieldType.array(FieldType.row(verticSchema)))
                  .withNullable(true))
          .collect(toSchema());

  public static final Schema latLonSchema =
      Stream.of(
              Schema.Field.of("latitude", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("longitude", FieldType.DOUBLE).withNullable(true))
          .collect(toSchema());

  public static final Schema locationSchema =
      Stream.of(Schema.Field.of("latLon", FieldType.row(latLonSchema)).withNullable(true))
          .collect(toSchema());

  public static final Schema labelAnnotationSchema =
      Stream.of(
              Schema.Field.of("gcsUri", FieldType.STRING).withNullable(true),
              Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
              Schema.Field.of("mid", FieldType.STRING).withNullable(true),
              Schema.Field.of("description", FieldType.STRING).withNullable(true),
              Schema.Field.of("score", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("topicality", FieldType.FLOAT).withNullable(true))
          .collect(toSchema());
  public static final Schema landmarkAnnotationSchema =
      Stream.of(
              Schema.Field.of("gcsUri", FieldType.STRING).withNullable(true),
              Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
              Schema.Field.of("mid", FieldType.STRING).withNullable(true),
              Schema.Field.of("description", FieldType.STRING).withNullable(true),
              Schema.Field.of("score", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("boundingPoly", FieldType.row(boundingPolySchema)).withNullable(true),
              Schema.Field.of(
                      "locations",
                      FieldType.array(FieldType.row(locationSchema)).withNullable(true))
                  .withNullable(true))
          .collect(toSchema());
  public static final Schema logoAnnotationSchema =
      Stream.of(
              Schema.Field.of("gcsUri", FieldType.STRING).withNullable(true),
              Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
              Schema.Field.of("mid", FieldType.STRING).withNullable(true),
              Schema.Field.of("description", FieldType.STRING).withNullable(true),
              Schema.Field.of("score", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("boundingPoly", FieldType.row(boundingPolySchema)).withNullable(true))
          .collect(toSchema());
  public static final Schema cropHintsAnnotationSchema =
      Stream.of(
              Schema.Field.of("gcsUri", FieldType.STRING).withNullable(true),
              Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
              Schema.Field.of("confidence", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("importanceFraction", FieldType.FLOAT).withNullable(true),
              Schema.Field.of("boundingPoly", FieldType.row(boundingPolySchema)).withNullable(true))
          .collect(toSchema());
  public static final Schema faceDetectionAnnotationSchema =
      Stream.of(
              Schema.Field.of("gcsUri", FieldType.STRING).withNullable(true),
              Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
              Schema.Field.of("boundingPoly", FieldType.row(boundingPolySchema)).withNullable(true),
              Schema.Field.of("fbBoundingPoly", FieldType.row(fdboundingPolySchema))
                  .withNullable(true),
              Schema.Field.of("landmarks", FieldType.array(FieldType.row(landmarkSchema)))
                  .withNullable(true))
          .collect(toSchema());

  public static final Schema imagePropertiesAnnotationSchema =
      Stream.of(
              Schema.Field.of("gcsUri", FieldType.STRING).withNullable(true),
              Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
              Schema.Field.of("dominantColors", FieldType.row(colorListSchema)).withNullable(true))
          .collect(toSchema());

  public static Row transformLabelAnnotations(String imageName, EntityAnnotation annotation) {
    Row row =
        Row.withSchema(labelAnnotationSchema)
            .addValues(
                imageName,
                "label_annotation",
                getTimeStamp(),
                annotation.getMid(),
                annotation.getDescription(),
                annotation.getScore(),
                annotation.getTopicality())
            .build();
    LOG.info("Row {}", row.toString());
    return row;
  }

  public static Row transformLandmarkAnnotations(String imageName, EntityAnnotation annotation) {
    Row row =
        Row.withSchema(landmarkAnnotationSchema)
            .addValues(
                imageName,
                "landmark_annotation",
                getTimeStamp(),
                annotation.getMid(),
                annotation.getDescription(),
                annotation.getScore(),
                Row.withSchema(boundingPolySchema)
                    .addValue(checkBoundingPolyForEntityAnnotation(annotation))
                    .build(),
                checkLocations(annotation))
            .build();
    LOG.info("Row {}", row.toString());
    return row;
  }

  public static Row transformFaceAnnotations(String imageName, FaceAnnotation annotation) {
    Row row =
        Row.withSchema(faceDetectionAnnotationSchema)
            .addValues(
                imageName,
                "face_annotation",
                getTimeStamp(),
                Row.withSchema(boundingPolySchema)
                    .addValue(checkBoundingPolyForFaceAnnotation(annotation))
                    .build(),
                Row.withSchema(fdboundingPolySchema)
                    .addValue(checkFDboundingPolyForFaceAnnotation(annotation))
                    .build(),
                checkLandmarksForFaceAnnotation(annotation))
            .build();

    LOG.info("Row {}", row.toString());
    return row;
  }

  public static Row transformLogoAnnotations(String imageName, EntityAnnotation annotation) {
    Row row =
        Row.withSchema(logoAnnotationSchema)
            .addValues(
                imageName,
                "logo_annotation",
                getTimeStamp(),
                annotation.getMid(),
                annotation.getDescription(),
                annotation.getScore(),
                Row.withSchema(boundingPolySchema)
                    .addValue(checkBoundingPolyForEntityAnnotation(annotation))
                    .build())
            .build();

    LOG.info("Row {}", row.toString());
    return row;
  }

  public static Row transformCropHintsAnnotations(String imageName, CropHint annotation) {

    Row row =
        Row.withSchema(cropHintsAnnotationSchema)
            .addValues(
                imageName,
                "corp_hints",
                getTimeStamp(),
                annotation.getConfidence(),
                annotation.getImportanceFraction(),
                Row.withSchema(boundingPolySchema)
                    .addValue(checkBoundingPolyForCorpHints(annotation))
                    .build())
            .build();
    LOG.debug("Row {}", row.toString());
    return row;
  }

  public static Row transformImagePropertiesAnnotations(
      String imageName, ImageProperties annotation) {

    Row row =
        Row.withSchema(imagePropertiesAnnotationSchema)
            .addValues(
                imageName,
                "image_properties",
                getTimeStamp(),
                Row.withSchema(colorListSchema)
                    .addValues(
                        (annotation.hasDominantColors())
                            ? checkDominantColorsForImageProperties(annotation.getDominantColors())
                            : null)
                    .build())
            .build();

    LOG.debug("Row {}", row.toString());
    return row;
  }

  private static List<Row> checkDominantColorsForImageProperties(
      DominantColorsAnnotation annotation) {
    List<Row> colorInfo = new ArrayList<>();
    annotation
        .getColorsList()
        .forEach(
            color -> {
              float red = color.getColor().getRed();
              float green = color.getColor().getGreen();
              float blue = color.getColor().getBlue();
              float score = color.getScore();
              float pixelFraction = color.getPixelFraction();
              Row c = Row.withSchema(colorSchema).addValues(red, green, blue).build();
              colorInfo.add(
                  Row.withSchema(colorInfoSchema).addValues(score, pixelFraction, c).build());
            });

    return colorInfo;
  }

  private static List<Row> checkBoundingPolyForCorpHints(CropHint corphint) {
    List<Row> verticesList = new ArrayList<>();
    if (corphint.hasBoundingPoly()) {
      corphint
          .getBoundingPoly()
          .getVerticesList()
          .forEach(
              vertics -> {
                verticesList.add(
                    Row.withSchema(verticSchema).addValues(vertics.getX(), vertics.getY()).build());
              });
    }

    return verticesList;
  }

  private static List<Row> checkBoundingPolyForEntityAnnotation(EntityAnnotation annotation) {
    List<Row> verticesList = new ArrayList<>();
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

    return verticesList;
  }

  private static List<Row> checkLocations(EntityAnnotation annotation) {
    List<Row> locationList = new ArrayList<>();
    if (annotation.getLocationsCount() > 0) {
      annotation
          .getLocationsList()
          .forEach(
              location -> {
                double latitude = location.getLatLng().getLatitude();
                double longitude = location.getLatLng().getLongitude();
                locationList.add(
                    Row.withSchema(locationSchema)
                        .addValues(
                            Row.withSchema(latLonSchema).addValues(latitude, longitude).build())
                        .build());
              });
    }
    return locationList;
  }

  private static List<Row> checkBoundingPolyForFaceAnnotation(FaceAnnotation annotation) {
    List<Row> verticesList = new ArrayList<>();
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

    return verticesList;
  }

  private static List<Row> checkFDboundingPolyForFaceAnnotation(FaceAnnotation annotation) {
    List<Row> verticesList = new ArrayList<>();
    if (annotation.hasFdBoundingPoly()) {
      annotation
          .getBoundingPoly()
          .getVerticesList()
          .forEach(
              vertics -> {
                verticesList.add(
                    Row.withSchema(verticSchema).addValues(vertics.getX(), vertics.getY()).build());
              });
    }

    return verticesList;
  }

  private static List<Row> checkLandmarksForFaceAnnotation(FaceAnnotation annotation) {
    List<Row> landmarkList = new ArrayList<>();

    annotation
        .getLandmarksList()
        .forEach(
            landmark -> {
              String type = landmark.getType().toString();
              float x = landmark.getPosition().getX();
              float y = landmark.getPosition().getY();
              float z = landmark.getPosition().getZ();
              landmarkList.add(
                  Row.withSchema(landmarkSchema)
                      .addValues(type, Row.withSchema(positionSchema).addValues(x, y, z).build())
                      .build());
            });

    return landmarkList;
  }

  public static String getTimeStamp() {
    return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));
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
        Iterable<?> items = (Iterable<?>) fieldValue;
        List<Object> convertedItems = Lists.newArrayListWithCapacity(Iterables.size(items));
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
