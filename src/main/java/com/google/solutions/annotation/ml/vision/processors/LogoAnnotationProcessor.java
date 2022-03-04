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
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.bigquery.BigQueryConstants;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.bigquery.TableDetails;
import com.google.solutions.annotation.bigquery.TableSchemaProducer;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import com.google.solutions.annotation.ml.Constants;
import com.google.solutions.annotation.ml.MLApiResponseProcessor;
import com.google.solutions.annotation.ml.ProcessorUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extracts logo annotations (https://cloud.google.com/vision/docs/detecting-logos) */
public class LogoAnnotationProcessor implements MLApiResponseProcessor {

  private static final long serialVersionUID = 1L;

  public static final Counter counter =
      Metrics.counter(MLApiResponseProcessor.class, "numberOfLogoAnnotations");
  public static final Logger LOG = LoggerFactory.getLogger(LogoAnnotationProcessor.class);

  private final BigQueryDestination destination;
  private final Set<String> metadataKeys;
  private final Set<String> relevantLogos;
  private final float scoreThreshold;

  /** Creates a processor and specifies the table id to persist to. */
  public LogoAnnotationProcessor(
      String tableId, Set<String> metadataKeys, Set<String> relevantLogos, float scoreThreshold) {
    this.destination = new BigQueryDestination(tableId);
    this.metadataKeys = metadataKeys;
    this.relevantLogos = relevantLogos;
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
        "Google Vision API Logo Annotations",
        new Clustering().setFields(Collections.singletonList(Constants.Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Constants.Field.TIMESTAMP_FIELD),
        new SchemaProducer(metadataKeys));
  }

  @Override
  public boolean shouldProcess(GeneratedMessageV3 response) {
    return (response instanceof AnnotateImageResponse
        && ((AnnotateImageResponse) response).getLogoAnnotationsCount() > 0);
  }

  @Override
  public ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 r) {
    AnnotateImageResponse response = (AnnotateImageResponse) r;
    counter.inc(response.getLogoAnnotationsCount());
    ProcessorResult result = new ProcessorResult(ProcessorResult.IMAGE_LOGO, destination);
    for (EntityAnnotation annotation : response.getLabelAnnotationsList()) {
      TableRow row = ProcessorUtils.startRow(fileInfo);
      row.put(Constants.Field.MID_FIELD, annotation.getMid());
      row.put(Constants.Field.DESCRIPTION_FIELD, annotation.getDescription());
      row.put(Constants.Field.SCORE_FIELD, annotation.getScore());
      ProcessorUtils.extractBoundingPoly(annotation, row);
      ProcessorUtils.addMetadataValues(row, fileInfo, metadataKeys);

      LOG.debug("Processing {}", row);
      result.allRows.add(row);

      if (relevantLogos != null
          && relevantLogos.stream().anyMatch(annotation.getDescription()::equalsIgnoreCase)
          && annotation.getScore() >= scoreThreshold) {
        result.relevantRows.add(row);
      }
    }
    return result;
  }
}
