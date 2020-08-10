/*
 * Copyright 2020 Google LLC
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

package com.google.solutions.ml.api.vision.processor;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.cloud.vision.v1.Position;
import com.google.common.collect.ImmutableList;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.TableDetails;
import com.google.solutions.ml.api.vision.TableSchemaProducer;
import com.google.solutions.ml.api.vision.processor.Constants.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts face annotations (https://cloud.google.com/vision/docs/detecting-faces)
 */
public class FaceAnnotationProcessor implements AnnotateImageResponseProcessor {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(FaceAnnotationProcessor.class);
  public final static Counter counter =
      Metrics.counter(AnnotateImageResponseProcessor.class, "numberOfFaceAnnotations");

  /**
   * The schema doesn't represent the complete list of all attributes returned by the APIs. For more
   * details see https://cloud.google.com/vision/docs/reference/rest/v1/AnnotateImageResponse?hl=pl#FaceAnnotation
   */
  private static class SchemaProducer implements TableSchemaProducer {

    private static final long serialVersionUID = 1L;

    @Override
    public TableSchema getTableSchema() {
      return new TableSchema().setFields(
          ImmutableList.of(
              new TableFieldSchema()
                  .setName(Field.GCS_URI_FIELD).setType("STRING").setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.BOUNDING_POLY).setType("RECORD")
                  .setMode("REQUIRED").setFields(Constants.POLYGON_FIELDS),
              new TableFieldSchema()
                  .setName(Field.FD_BOUNDING_POLY).setType("RECORD")
                  .setMode("REQUIRED").setFields(Constants.POLYGON_FIELDS),
              new TableFieldSchema()
                  .setName(Field.LANDMARKS).setType("RECORD").setMode("REPEATED").setFields(
                  Arrays.asList(
                      new TableFieldSchema().setName(Field.FACE_LANDMARK_TYPE).setType("STRING")
                          .setMode("REQUIRED"),
                      new TableFieldSchema().setName(Field.FACE_LANDMARK_POSITION).setType("RECORD")
                          .setMode("REQUIRED").setFields(Constants.POSITION_FIELDS)
                  )
              ),
              new TableFieldSchema()
                  .setName(Field.DETECTION_CONFIDENCE).setType("FLOAT").setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.LANDMARKING_CONFIDENCE).setType("FLOAT").setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.JOY_LIKELIHOOD).setType("STRING").setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.SORROW_LIKELIHOOD).setType("STRING").setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.ANGER_LIKELIHOOD).setType("STRING").setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.SURPISE_LIKELIHOOD).setType("STRING").setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.TIMESTAMP_FIELD).setType("TIMESTAMP")
                  .setMode("REQUIRED"))
      );
    }
  }

  @Override
  public TableDetails destinationTableDetails() {
    return TableDetails.create("Google Vision API Face Annotations",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD), new SchemaProducer());
  }

  private final BQDestination destination;

  /**
   * Creates a processor and specifies the table id to persist to.
   */
  public FaceAnnotationProcessor(String tableId) {
    destination = new BQDestination(tableId);
  }

  @Override
  public Iterable<KV<BQDestination, TableRow>> process(
      String gcsURI, AnnotateImageResponse response) {
    int numberOfAnnotations = response.getFaceAnnotationsCount();
    if (numberOfAnnotations == 0) {
      return null;
    }

    counter.inc(numberOfAnnotations);

    Collection<KV<BQDestination, TableRow>> result = new ArrayList<>(numberOfAnnotations);
    for (FaceAnnotation annotation : response.getFaceAnnotationsList()) {
      TableRow row = ProcessorUtils.startRow(gcsURI);

      row.put(Field.BOUNDING_POLY,
          ProcessorUtils.getBoundingPolyAsRow(annotation.getBoundingPoly()));
      row.put(Field.FD_BOUNDING_POLY,
          ProcessorUtils.getBoundingPolyAsRow(annotation.getFdBoundingPoly()));
      List<TableRow> landmarks = new ArrayList<>(annotation.getLandmarksCount());
      annotation.getLandmarksList().forEach(
          landmark -> {
            TableRow landmarkRow = new TableRow();
            landmarkRow.put(Field.FACE_LANDMARK_TYPE, landmark.getType().toString());

            Position position = landmark.getPosition();
            TableRow positionRow = new TableRow();
            positionRow.put(Field.VERTEX_X, position.getX());
            positionRow.put(Field.VERTEX_Y, position.getY());
            positionRow.put(Field.VERTEX_Z, position.getZ());
            landmarkRow.put(Field.FACE_LANDMARK_POSITION, positionRow);

            landmarks.add(landmarkRow);
          }
      );
      row.put(Field.LANDMARKS, landmarks);
      row.put(Field.DETECTION_CONFIDENCE, annotation.getDetectionConfidence());
      row.put(Field.LANDMARKING_CONFIDENCE, annotation.getLandmarkingConfidence());
      row.put(Field.JOY_LIKELIHOOD, annotation.getJoyLikelihood().toString());
      row.put(Field.SORROW_LIKELIHOOD, annotation.getSorrowLikelihood().toString());
      row.put(Field.ANGER_LIKELIHOOD, annotation.getAngerLikelihood().toString());
      row.put(Field.SURPISE_LIKELIHOOD, annotation.getSurpriseLikelihood().toString());

      LOG.debug("Processing {}", row);
      result.add(KV.of(destination, row));
    }

    return result;
  }

}
