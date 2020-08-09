package com.google.solutions.ml.api.vision.processor;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.BoundingPoly;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.solutions.ml.api.vision.common.Util;
import com.google.solutions.ml.api.vision.processor.Constants.Field;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class ProcessorUtils {

  static void extractBoundingPoly(EntityAnnotation annotation, TableRow row) {
    if (annotation.hasBoundingPoly()) {
      TableRow boundingPoly = getBoundingPolyAsRow(annotation.getBoundingPoly());
      row.put(Field.BOUNDING_POLY, boundingPoly);
    }
  }

  static TableRow getBoundingPolyAsRow(BoundingPoly boundingPoly) {
    List<TableRow> vertices = new ArrayList<>();
    boundingPoly.getVerticesList()
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

  static TableRow startRow(String gcsURI) {
    TableRow row = new TableRow();
    row.put(Field.GCS_URI_FIELD, gcsURI);
    row.put(Field.TIMESTAMP_FIELD, getTimeStamp());
    return row;
  }

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static String getTimeStamp() {
    return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));
  }
}
