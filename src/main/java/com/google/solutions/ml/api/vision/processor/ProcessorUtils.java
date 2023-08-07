/*
 * Copyright 2023 Google LLC
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
package com.google.solutions.ml.api.vision.processor;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.BoundingPoly;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.solutions.ml.api.vision.processor.Constants.Field;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Various utility functions used by processors */
public class ProcessorUtils {

  /** Extracts the bounding polygon if one exists and adds it to the row. */
  static void extractBoundingPoly(EntityAnnotation annotation, TableRow row) {
    if (annotation.hasBoundingPoly()) {
      TableRow boundingPoly = getBoundingPolyAsRow(annotation.getBoundingPoly());
      row.put(Field.BOUNDING_POLY, boundingPoly);
    }
  }

  /**
   * Converts {@link BoundingPoly} to a {@link TableRow}.
   *
   * @return table row
   */
  static TableRow getBoundingPolyAsRow(BoundingPoly boundingPoly) {
    List<TableRow> vertices = new ArrayList<>();
    boundingPoly
        .getVerticesList()
        .forEach(
            vertex -> {
              TableRow vertexRow = new TableRow();
              vertexRow.put(Field.VERTEX_X, vertex.getX());
              vertexRow.put(Field.VERTEX_Y, vertex.getY());
              vertices.add(vertexRow);
            });
    TableRow result = new TableRow();
    result.put(Field.VERTICES, vertices);
    return result;
  }

  /**
   * Creates a TableRow and populates with two fields used in all processors: {@link
   * Constants.Field#GCS_URI_FIELD} and {@link Constants.Field#TIMESTAMP_FIELD}
   *
   * @return new TableRow
   */
  static TableRow startRow(String gcsURI) {
    TableRow row = new TableRow();
    row.put(Field.GCS_URI_FIELD, gcsURI);
    row.put(Field.TIMESTAMP_FIELD, getTimeStamp());
    return row;
  }

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  /** Formats the current timestamp in BigQuery compliant format */
  public static String getTimeStamp() {
    return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));
  }
}
