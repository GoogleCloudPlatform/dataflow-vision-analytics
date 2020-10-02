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
import com.google.common.collect.ImmutableList;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.BigQueryConstants.Mode;
import com.google.solutions.ml.api.vision.BigQueryConstants.Type;
import com.google.solutions.ml.api.vision.TableDetails;
import com.google.solutions.ml.api.vision.TableSchemaProducer;
import com.google.solutions.ml.api.vision.processor.Constants.Field;
import java.util.Collections;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Captures the error occurred during processing. Note, that there could be some valid annotations
 * returned in the response even though the response contains an error.
 */
public class ErrorProcessor implements AnnotateImageResponseProcessor {

  private static final long serialVersionUID = 1L;

  public final static Counter counter =
      Metrics.counter(AnnotateImageResponseProcessor.class, "numberOfErrors");
  public static final Logger LOG = LoggerFactory.getLogger(ErrorProcessor.class);

  private static class SchemaProducer implements TableSchemaProducer {

    private static final long serialVersionUID = 1L;

    @Override
    public TableSchema getTableSchema() {
      return new TableSchema().setFields(
          ImmutableList.of(
              new TableFieldSchema()
                  .setName(Field.GCS_URI_FIELD)
                  .setType(Type.STRING)
                  .setMode(Mode.REQUIRED),
              new TableFieldSchema()
                  .setName(Field.DESCRIPTION_FIELD).setType(Type.STRING)
                  .setMode(Mode.REQUIRED),
              new TableFieldSchema()
                  .setName(Field.STACK_TRACE).setType(Type.STRING)
                  .setMode(Mode.NULLABLE),
              new TableFieldSchema()
                  .setName(Field.TIMESTAMP_FIELD).setType(Type.TIMESTAMP)
                  .setMode(Mode.REQUIRED))
      );
    }
  }

  @Override
  public TableDetails destinationTableDetails() {
    return TableDetails.create("Google Vision API Processing Errors",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD), new SchemaProducer());
  }

  private final BQDestination destination;

  /**
   * Creates a processor and specifies the table id to persist to.
   */
  public ErrorProcessor(String tableId) {
    destination = new BQDestination(tableId);
  }

  @Override
  public Iterable<KV<BQDestination, TableRow>> process(
      String gcsURI, AnnotateImageResponse response) {
    if (!response.hasError()) {
      return null;
    }

    counter.inc();

    TableRow result = ProcessorUtils.startRow(gcsURI);
    result.put(Field.DESCRIPTION_FIELD, response.getError().toString());

    LOG.debug("Processing {}", result);

    return Collections.singletonList(KV.of(destination, result));
  }
}
