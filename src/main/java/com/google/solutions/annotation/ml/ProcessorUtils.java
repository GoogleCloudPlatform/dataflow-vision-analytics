/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.solutions.annotation.ml;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.BoundingPoly;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Various utility functions used by processors */
public class ProcessorUtils {

  /** Extracts the bounding polygon if one exists and adds it to the row. */
  public static void extractBoundingPoly(EntityAnnotation annotation, TableRow row) {
    if (annotation.hasBoundingPoly()) {
      TableRow boundingPoly = getBoundingPolyAsRow(annotation.getBoundingPoly());
      row.put(Constants.Field.BOUNDING_POLY, boundingPoly);
    }
  }

  /**
   * Converts {@link BoundingPoly} to a {@link TableRow}.
   *
   * @return table row
   */
  public static TableRow getBoundingPolyAsRow(BoundingPoly boundingPoly) {
    List<TableRow> vertices = new ArrayList<>();
    boundingPoly
        .getVerticesList()
        .forEach(
            vertex -> {
              TableRow vertexRow = new TableRow();
              vertexRow.put(Constants.Field.VERTEX_X, vertex.getX());
              vertexRow.put(Constants.Field.VERTEX_Y, vertex.getY());
              vertices.add(vertexRow);
            });
    TableRow result = new TableRow();
    result.put(Constants.Field.VERTICES, vertices);
    return result;
  }

  /**
   * Creates a TableRow and populates with two fields used in all processors: {@link
   * Constants.Field#GCS_URI_FIELD} and {@link Constants.Field#TIMESTAMP_FIELD}
   *
   * @return new TableRow
   */
  public static TableRow startRow(GCSFileInfo fileInfo) {
    TableRow row = new TableRow();
    row.put(Constants.Field.GCS_URI_FIELD, fileInfo.getUri());
    row.put(Constants.Field.TIMESTAMP_FIELD, getTimeStamp());
    return row;
  }

  public static void setMetadataFieldsSchema(
      List<TableFieldSchema> fields, Set<String> metadataKeys) {
    if (!metadataKeys.isEmpty()) {
      List<TableFieldSchema> metadataFields = new ArrayList<>();
      for (String key : metadataKeys) {
        metadataFields.add(new TableFieldSchema().setName(key).setType("STRING"));
      }
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.METADATA)
              .setType("RECORD")
              .setFields(metadataFields));
    }
  }

  public static void addMetadataValues(
      TableRow row, GCSFileInfo fileInfo, Set<String> metadataKeys) {
    // Add metadata to the row, if any
    TableRow metadataRow = new TableRow();
    if (fileInfo.getMetadata() != null) {
      for (String key : metadataKeys) {
        String value = fileInfo.getMetadata().get(key);
        if (value != null) {
          metadataRow.put(key, value);
        }
      }
    }
    if (!metadataRow.isEmpty()) {
      row.put(Constants.Field.METADATA, metadataRow);
    }
  }

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  /** Formats the current timestamp in BigQuery compliant format */
  public static String getTimeStamp() {
    return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));
  }
}
