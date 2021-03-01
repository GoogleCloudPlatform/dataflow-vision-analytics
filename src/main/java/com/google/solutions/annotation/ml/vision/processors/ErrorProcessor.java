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
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.ml.Constants.Field;
import com.google.solutions.annotation.bigquery.BigQueryConstants;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.bigquery.TableDetails;
import com.google.solutions.annotation.bigquery.TableSchemaProducer;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import com.google.solutions.annotation.ml.MLApiResponseProcessor;
import com.google.solutions.annotation.ml.ProcessorUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Captures the error occurred during processing. Note, that there could be some valid annotations
 * returned in the response even though the response contains an error.
 */
public class ErrorProcessor implements MLApiResponseProcessor {

  private static final long serialVersionUID = 1L;

  public static final Counter counter =
      Metrics.counter(MLApiResponseProcessor.class, "numberOfErrors");
  public static final Logger LOG = LoggerFactory.getLogger(ErrorProcessor.class);

  private final BigQueryDestination destination;
  private final Set<String> metadataKeys;

  /** Creates a processor and specifies the table id to persist to. */
  public ErrorProcessor(String tableId, Set<String> metadataKeys) {
    this.destination = new BigQueryDestination(tableId);
    this.metadataKeys = metadataKeys;
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
              .setName(Field.DESCRIPTION_FIELD)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.REQUIRED));
      fields.add(
          new TableFieldSchema()
              .setName(Field.STACK_TRACE)
              .setType(BigQueryConstants.Type.STRING)
              .setMode(BigQueryConstants.Mode.NULLABLE));
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
        "Google Vision API Processing Errors",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD),
        new SchemaProducer(metadataKeys));
  }

  @Override
  public boolean shouldProcess(GeneratedMessageV3 response) {
    return (response instanceof AnnotateImageResponse
        && ((AnnotateImageResponse) response).hasError());
  }

  @Override
  public ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 r) {
    AnnotateImageResponse response = (AnnotateImageResponse) r;
    counter.inc();

    TableRow row = ProcessorUtils.startRow(fileInfo);
    row.put(Field.DESCRIPTION_FIELD, response.getError().toString());
    ProcessorUtils.addMetadataValues(row, fileInfo, metadataKeys);

    LOG.debug("Processing {}", row);

    ProcessorResult result = new ProcessorResult(ProcessorResult.IMAGE_ERROR, destination);
    result.allRows.add(row);
    return result;
  }
}
