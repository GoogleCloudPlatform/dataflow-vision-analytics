/*
 * Copyright 2021 Google LLC
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
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.ml.Constants;
import com.google.solutions.annotation.bigquery.BigQueryConstants;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.bigquery.TableDetails;
import com.google.solutions.annotation.bigquery.TableSchemaProducer;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import com.google.solutions.annotation.ml.MLApiResponseProcessor;
import com.google.solutions.annotation.ml.ProcessorUtils;
import java.util.*;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extracts landmark annotations (https://cloud.google.com/vision/docs/detecting-landmarks) */
public class LandmarkAnnotationProcessor implements MLApiResponseProcessor {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(LandmarkAnnotationProcessor.class);
  public static final Counter counter =
      Metrics.counter(MLApiResponseProcessor.class, "numberOfLandmarkAnnotations");

  private final BigQueryDestination destination;
  private final Set<String> metadataKeys;
  private final Set<String> relevantLandmarks;
  private final float scoreThreshold;

  /** Creates a processor and specifies the table id to persist to. */
  public LandmarkAnnotationProcessor(
      String tableId,
      Set<String> metadataKeys,
      Set<String> relevantLandmarks,
      float scoreThreshold) {
    this.destination = new BigQueryDestination(tableId);
    this.metadataKeys = metadataKeys;
    this.relevantLandmarks = relevantLandmarks;
    this.scoreThreshold = scoreThreshold;
  }

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
              .setName(Constants.Field.MID_FIELD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.NULLABLE));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.DESCRIPTION_FIELD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.SCORE_FIELD)
              .setType(BigQueryConstants.Type.FLOAT)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.BOUNDING_POLY)
              .setType(BigQueryConstants.Type.RECORD)
              .setMode(BigQueryConstants.Mode.NULLABLE)
              .setFields(Constants.POLYGON_FIELDS));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.LOCATIONS)
              .setType(BigQueryConstants.Type.GEOGRAPHY)
              .setMode(BigQueryConstants.Mode.REPEATED));
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
        "Google Vision API Landmark Annotations",
        new Clustering().setFields(Collections.singletonList(Constants.Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Constants.Field.TIMESTAMP_FIELD),
        new SchemaProducer(metadataKeys));
  }

  @Override
  public boolean shouldProcess(GeneratedMessageV3 response) {
    return (response instanceof AnnotateImageResponse
        && ((AnnotateImageResponse) response).getLandmarkAnnotationsCount() > 0);
  }

  @Override
  public ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 r) {
    AnnotateImageResponse response = (AnnotateImageResponse) r;
    counter.inc(response.getLandmarkAnnotationsCount());
    ProcessorResult result = new ProcessorResult(ProcessorResult.IMAGE_LANDMARK, destination);
    for (EntityAnnotation annotation : response.getLandmarkAnnotationsList()) {
      TableRow row = ProcessorUtils.startRow(fileInfo);
      row.put(Constants.Field.MID_FIELD, annotation.getMid());
      row.put(Constants.Field.DESCRIPTION_FIELD, annotation.getDescription());
      row.put(Constants.Field.SCORE_FIELD, annotation.getScore());

      ProcessorUtils.extractBoundingPoly(annotation, row);

      if (annotation.getLocationsCount() > 0) {
        List<String> locations = new ArrayList<>(annotation.getLocationsCount());
        annotation
            .getLocationsList()
            .forEach(
                location ->
                    locations.add(
                        "POINT("
                            + location.getLatLng().getLongitude()
                            + " "
                            + location.getLatLng().getLatitude()
                            + ")"));
        row.put(Constants.Field.LOCATIONS, locations);
      }

      ProcessorUtils.addMetadataValues(row, fileInfo, metadataKeys);

      LOG.debug("Processing {}", row);
      result.allRows.add(row);

      if (relevantLandmarks != null
          && relevantLandmarks.stream().anyMatch(annotation.getDescription()::equalsIgnoreCase)
          && annotation.getScore() >= scoreThreshold) {
        result.relevantRows.add(row);
      }
    }

    return result;
  }
}
