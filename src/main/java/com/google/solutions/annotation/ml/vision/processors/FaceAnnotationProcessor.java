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
package com.google.solutions.annotation.ml.vision.processors;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.cloud.vision.v1.Position;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.bigquery.BigQueryConstants;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.bigquery.TableDetails;
import com.google.solutions.annotation.bigquery.TableSchemaProducer;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import com.google.solutions.annotation.ml.Constants;
import com.google.solutions.annotation.ml.MLApiResponseProcessor;
import com.google.solutions.annotation.ml.ProcessorUtils;
import java.util.*;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extracts face annotations (https://cloud.google.com/vision/docs/detecting-faces) */
public class FaceAnnotationProcessor implements MLApiResponseProcessor {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(FaceAnnotationProcessor.class);
  public static final Counter counter =
      Metrics.counter(MLApiResponseProcessor.class, "numberOfFaceAnnotations");
  private final BigQueryDestination destination;
  private final Set<String> metadataKeys;
  private final float detectionConfidenceThreshold;

  /** Creates a processor and specifies the table id to persist to. */
  public FaceAnnotationProcessor(
      String tableId, Set<String> metadataKeys, float detectionConfidenceThreshold) {
    this.destination = new BigQueryDestination(tableId);
    this.metadataKeys = metadataKeys;
    this.detectionConfidenceThreshold = detectionConfidenceThreshold;
  }

  /**
   * The schema doesn't represent the complete list of all attributes returned by the APIs. For more
   * details see
   * https://cloud.google.com/vision/docs/reference/rest/v1/AnnotateImageResponse?hl=pl#FaceAnnotation
   */
  private static class SchemaProducer implements TableSchemaProducer {

    private static final long serialVersionUID = 1L;
    private final Set<String> metadataKeys;

    SchemaProducer(Set<String> metadataKeys) {
      this.metadataKeys = metadataKeys;
    }

    @Override
    public TableSchema getTableSchema() {
      ArrayList<TableFieldSchema> fields = new ArrayList<>();
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.GCS_URI_FIELD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.BOUNDING_POLY)
              .setType(BigQueryConstants.Type.RECORD)
              .setMode(BigQueryConstants.Mode.REQUIRED)
              .setFields(Constants.POLYGON_FIELDS));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.FD_BOUNDING_POLY)
              .setType(BigQueryConstants.Type.RECORD)
              .setMode(BigQueryConstants.Mode.REQUIRED)
              .setFields(Constants.POLYGON_FIELDS));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.LANDMARKS)
              .setType(BigQueryConstants.Type.RECORD)
              .setMode(BigQueryConstants.Mode.REPEATED)
              .setFields(
                  Arrays.asList(
                      new TableFieldSchema()
                          .setName(Constants.Field.FACE_LANDMARK_TYPE)
                          .setType(BigQueryConstants.Type.STRING)
                          .setMode(BigQueryConstants.Mode.REQUIRED),
                      new TableFieldSchema()
                          .setName(Constants.Field.FACE_LANDMARK_POSITION)
                          .setType(BigQueryConstants.Type.RECORD)
                          .setMode(BigQueryConstants.Mode.REQUIRED)
                          .setFields(Constants.POSITION_FIELDS))));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.DETECTION_CONFIDENCE)
              .setType(BigQueryConstants.Type.FLOAT)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.LANDMARKING_CONFIDENCE)
              .setType(BigQueryConstants.Type.FLOAT)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.JOY_LIKELIHOOD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.SORROW_LIKELIHOOD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.ANGER_LIKELIHOOD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.SURPRISE_LIKELIHOOD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.TIMESTAMP_FIELD)
              .setType(BigQueryConstants.Type.TIMESTAMP)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      ProcessorUtils.setMetadataFieldsSchema(fields, metadataKeys);
      return new TableSchema().setFields(fields);
    }
  }

  @Override
  public TableDetails destinationTableDetails() {
    return TableDetails.create(
        "Google Vision API Face Annotations",
        new Clustering().setFields(Collections.singletonList(Constants.Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Constants.Field.TIMESTAMP_FIELD),
        new SchemaProducer(metadataKeys));
  }

  @Override
  public boolean shouldProcess(GeneratedMessageV3 response) {
    return (response instanceof AnnotateImageResponse
        && ((AnnotateImageResponse) response).getFaceAnnotationsCount() > 0);
  }

  @Override
  public ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 r) {
    AnnotateImageResponse response = (AnnotateImageResponse) r;
    counter.inc(response.getFaceAnnotationsCount());
    ProcessorResult result = new ProcessorResult(ProcessorResult.IMAGE_FACE, destination);
    for (FaceAnnotation annotation : response.getFaceAnnotationsList()) {
      TableRow row = ProcessorUtils.startRow(fileInfo);

      row.put(
          Constants.Field.BOUNDING_POLY,
          ProcessorUtils.getBoundingPolyAsRow(annotation.getBoundingPoly()));
      row.put(
          Constants.Field.FD_BOUNDING_POLY,
          ProcessorUtils.getBoundingPolyAsRow(annotation.getFdBoundingPoly()));
      List<TableRow> landmarks = new ArrayList<>(annotation.getLandmarksCount());
      annotation
          .getLandmarksList()
          .forEach(
              landmark -> {
                TableRow landmarkRow = new TableRow();
                landmarkRow.put(Constants.Field.FACE_LANDMARK_TYPE, landmark.getType().toString());

                Position position = landmark.getPosition();
                TableRow positionRow = new TableRow();
                positionRow.put(Constants.Field.VERTEX_X, position.getX());
                positionRow.put(Constants.Field.VERTEX_Y, position.getY());
                positionRow.put(Constants.Field.VERTEX_Z, position.getZ());
                landmarkRow.put(Constants.Field.FACE_LANDMARK_POSITION, positionRow);

                landmarks.add(landmarkRow);
              });
      row.put(Constants.Field.LANDMARKS, landmarks);
      row.put(Constants.Field.DETECTION_CONFIDENCE, annotation.getDetectionConfidence());
      row.put(Constants.Field.LANDMARKING_CONFIDENCE, annotation.getLandmarkingConfidence());
      row.put(Constants.Field.JOY_LIKELIHOOD, annotation.getJoyLikelihood().toString());
      row.put(Constants.Field.SORROW_LIKELIHOOD, annotation.getSorrowLikelihood().toString());
      row.put(Constants.Field.ANGER_LIKELIHOOD, annotation.getAngerLikelihood().toString());
      row.put(Constants.Field.SURPRISE_LIKELIHOOD, annotation.getSurpriseLikelihood().toString());
      ProcessorUtils.addMetadataValues(row, fileInfo, metadataKeys);

      LOG.debug("Processing {}", row);
      result.allRows.add(row);

      if (annotation.getDetectionConfidence() >= detectionConfidenceThreshold) {
        result.relevantRows.add(row);
      }
    }

    return result;
  }
}
