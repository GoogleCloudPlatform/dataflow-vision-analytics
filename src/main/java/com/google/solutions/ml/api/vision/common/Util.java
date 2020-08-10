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

import com.google.cloud.vision.v1.CropHint;
import com.google.cloud.vision.v1.DominantColorsAnnotation;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class to reuse some convert methods used in the pipeline multiple times
 */
@SuppressWarnings("serial")
public class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);
  /**
   * Error message if no valid extension found
   */
  public static final String NO_VALID_EXT_FOUND_ERROR_MESSAGE =
      "File {} does not contain a valid extension";



//  public static final Schema colorSchema =
//      Stream.of(
//          Schema.Field.of("red", FieldType.FLOAT).withNullable(true),
//          Schema.Field.of("green", FieldType.FLOAT).withNullable(true),
//          Schema.Field.of("blue", FieldType.FLOAT).withNullable(true))
//          .collect(toSchema());
//  public static final Schema colorInfoSchema =
//      Stream.of(
//          Schema.Field.of(Field.SCORE_FIELD, FieldType.FLOAT).withNullable(true),
//          Schema.Field.of("pixelFraction", FieldType.FLOAT).withNullable(true),
//          Schema.Field.of("color", FieldType.row(colorSchema)).withNullable(true))
//          .collect(toSchema());
//  public static final Schema colorListSchema =
//      Stream.of(
//          Schema.Field.of("colors", FieldType.array(FieldType.row(colorInfoSchema)))
//              .withNullable(true))
//          .collect(toSchema());



//  public static final Schema cropHintsAnnotationSchema =
//      Stream.of(
//          Schema.Field.of(Constants.Field.GCS_URI_FIELD, FieldType.STRING).withNullable(true),
//          Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
//          Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
//          Schema.Field.of("confidence", FieldType.FLOAT).withNullable(true),
//          Schema.Field.of("importanceFraction", FieldType.FLOAT).withNullable(true),
//          Schema.Field.of("boundingPoly", FieldType.row(boundingPolySchema)).withNullable(true))
//          .collect(toSchema());
//  public static final Schema faceDetectionAnnotationSchema =
//      Stream.of(
//          Schema.Field.of(Constants.Field.GCS_URI_FIELD, FieldType.STRING).withNullable(true),
//          Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
//          Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
//          Schema.Field.of("boundingPoly", FieldType.row(boundingPolySchema)).withNullable(true),
//          Schema.Field.of("fbBoundingPoly", FieldType.row(fdboundingPolySchema))
//              .withNullable(true),
//          Schema.Field.of("landmarks", FieldType.array(FieldType.row(landmarkSchema)))
//              .withNullable(true))
//          .collect(toSchema());
//
//  public static final Schema imagePropertiesAnnotationSchema =
//      Stream.of(
//          Schema.Field.of(Constants.Field.GCS_URI_FIELD, FieldType.STRING).withNullable(true),
//          Schema.Field.of("feature_type", FieldType.STRING).withNullable(true),
//          Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
//          Schema.Field.of("dominantColors", FieldType.row(colorListSchema)).withNullable(true))
//          .collect(toSchema());
//
//  public static TableRow labelAnnotationToTableRow(String imageName, EntityAnnotation annotation) {
//    TableRow result = new TableRow();
//    result.put(Constants.Field.GCS_URI_FIELD, imageName);
//    result.put(Constants.Field.TIMESTAMP_FIELD, getTimeStamp());
//    result.put(Constants.Field.MID_FIELD, annotation.getMid());
//    result.put(Constants.Field.DESCRIPTION_FIELD, annotation.getDescription());
//    result.put(SCORE_FIELD, annotation.getScore());
//    result.put(Constants.Field.TOPICALITY_FIELD, annotation.getTopicality());
//
//    LOG.debug("Processing {}", result);
//    return result;
//  }

//  public static Row transformCropHintsAnnotations(String imageName, CropHint annotation) {
//
//    Row row =
//        Row.withSchema(cropHintsAnnotationSchema)
//            .addValues(
//                imageName,
//                "corp_hints",
//                getTimeStamp(),
//                annotation.getConfidence(),
//                annotation.getImportanceFraction(),
//                Row.withSchema(boundingPolySchema)
//                    .addValue(checkBoundingPolyForCorpHints(annotation))
//                    .build())
//            .build();
//    LOG.debug("Row {}", row.toString());
//    return row;
//  }

//  public static Row transformImagePropertiesAnnotations(
//      String imageName, ImageProperties annotation) {
//
//    Row row =
//        Row.withSchema(imagePropertiesAnnotationSchema)
//            .addValues(
//                imageName,
//                "image_properties",
//                getTimeStamp(),
//                Row.withSchema(colorListSchema)
//                    .addValues(
//                        (annotation.hasDominantColors())
//                            ? checkDominantColorsForImageProperties(annotation.getDominantColors())
//                            : null)
//                    .build())
//            .build();
//
//    LOG.debug("Row {}", row.toString());
//    return row;
//  }

//  private static List<Row> checkDominantColorsForImageProperties(
//      DominantColorsAnnotation annotation) {
//    List<Row> colorInfo = new ArrayList<>();
//    annotation
//        .getColorsList()
//        .forEach(
//            color -> {
//              float red = color.getColor().getRed();
//              float green = color.getColor().getGreen();
//              float blue = color.getColor().getBlue();
//              float score = color.getScore();
//              float pixelFraction = color.getPixelFraction();
//              Row c = Row.withSchema(colorSchema).addValues(red, green, blue).build();
//              colorInfo.add(
//                  Row.withSchema(colorInfoSchema).addValues(score, pixelFraction, c).build());
//            });
//
//    return colorInfo;
//  }

//  private static List<Row> checkBoundingPolyForCorpHints(CropHint corphint) {
//    List<Row> verticesList = new ArrayList<>();
//    if (corphint.hasBoundingPoly()) {
//      corphint
//          .getBoundingPoly()
//          .getVerticesList()
//          .forEach(
//              vertics -> {
//                verticesList.add(
//                    Row.withSchema(verticSchema).addValues(vertics.getX(), vertics.getY()).build());
//              });
//    }
//
//    return verticesList;
//  }


}
