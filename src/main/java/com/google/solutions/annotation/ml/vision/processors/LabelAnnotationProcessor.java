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
import com.google.solutions.annotation.ml.Constants.Field;
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

/** Extracts label annotations (https://cloud.google.com/vision/docs/labels) */
public class LabelAnnotationProcessor implements MLApiResponseProcessor {

  private static final long serialVersionUID = 1L;
  private final BigQueryDestination destination;
  private final Set<String> metadataKeys;
  private final Set<String> relevantLabels;
  private final float scoreThreshold;
  private static final Logger LOG = LoggerFactory.getLogger(LabelAnnotationProcessor.class);

  public static final Counter counter =
      Metrics.counter(MLApiResponseProcessor.class, "numberOfLabelAnnotations");

  /** Creates a processor and specifies the table id to persist to. */
  public LabelAnnotationProcessor(
      String tableId, Set<String> metadataKeys, Set<String> relevantLabels, float scoreThreshold) {
    this.destination = new BigQueryDestination(tableId);
    this.metadataKeys = metadataKeys;
    this.relevantLabels = relevantLabels;
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
              .setName(Field.GCS_URI_FIELD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Field.MID_FIELD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.NULLABLE));
      fields.add(
          new TableFieldSchema()
              .setName(Field.DESCRIPTION_FIELD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Field.SCORE_FIELD)
              .setType(BigQueryConstants.Type.FLOAT)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Field.TOPICALITY_FIELD)
              .setType(BigQueryConstants.Type.FLOAT)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Field.TIMESTAMP_FIELD)
              .setType(BigQueryConstants.Type.TIMESTAMP)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      ProcessorUtils.setMetadataFieldsSchema(fields, metadataKeys);
      return new TableSchema().setFields(fields);
    }
  }

  @Override
  public TableDetails destinationTableDetails() {
    return TableDetails.create(
        "Google Vision API Label Annotations",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD),
        new SchemaProducer(metadataKeys));
  }

  @Override
  public boolean shouldProcess(GeneratedMessageV3 response) {
    return (response instanceof AnnotateImageResponse
        && ((AnnotateImageResponse) response).getLabelAnnotationsCount() > 0);
  }

  @Override
  public ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 r) {
    AnnotateImageResponse response = (AnnotateImageResponse) r;
    counter.inc(response.getLabelAnnotationsCount());
    ProcessorResult result = new ProcessorResult(ProcessorResult.IMAGE_LABEL, destination);
    for (EntityAnnotation annotation : response.getLabelAnnotationsList()) {
      TableRow row = ProcessorUtils.startRow(fileInfo);
      row.put(Field.MID_FIELD, annotation.getMid());
      row.put(Field.DESCRIPTION_FIELD, annotation.getDescription());
      row.put(Field.SCORE_FIELD, annotation.getScore());
      row.put(Field.TOPICALITY_FIELD, annotation.getTopicality());
      ProcessorUtils.addMetadataValues(row, fileInfo, metadataKeys);

      LOG.debug("Processing {}", row);
      result.allRows.add(row);

      if (relevantLabels != null
          && relevantLabels.stream().anyMatch(annotation.getDescription()::equalsIgnoreCase)
          && annotation.getScore() >= scoreThreshold) {
        result.relevantRows.add(row);
      }
    }
    return result;
  }
}
