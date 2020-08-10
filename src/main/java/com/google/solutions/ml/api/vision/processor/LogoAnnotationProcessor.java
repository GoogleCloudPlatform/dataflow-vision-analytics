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
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.common.collect.ImmutableList;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.TableDetails;
import com.google.solutions.ml.api.vision.TableSchemaProducer;
import com.google.solutions.ml.api.vision.processor.Constants.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts logo annotations (https://cloud.google.com/vision/docs/detecting-logos)
 */
public class LogoAnnotationProcessor implements AnnotateImageResponseProcessor {

  private static final long serialVersionUID = 1L;

  public final static Counter counter =
      Metrics.counter(AnnotateImageResponseProcessor.class, "numberOfLogoAnnotations");
  public static final Logger LOG = LoggerFactory.getLogger(LogoAnnotationProcessor.class);

  private static class SchemaProducer implements TableSchemaProducer {

    private static final long serialVersionUID = 1L;

    @Override
    public TableSchema getTableSchema() {
      return new TableSchema().setFields(
          ImmutableList.of(
              new TableFieldSchema()
                  .setName(Field.GCS_URI_FIELD)
                  .setType("STRING")
                  .setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.MID_FIELD).setType("STRING")
                  .setMode("NULLABLE"),
              new TableFieldSchema()
                  .setName(Field.DESCRIPTION_FIELD).setType("STRING")
                  .setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.SCORE_FIELD).setType("FLOAT")
                  .setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.BOUNDING_POLY).setType("RECORD")
                  .setMode("NULLABLE").setFields(Constants.POLYGON_FIELDS),
              new TableFieldSchema()
                  .setName(Field.TIMESTAMP_FIELD).setType("TIMESTAMP")
                  .setMode("REQUIRED"))
      );
    }
  }

  @Override
  public TableDetails destinationTableDetails() {
    return TableDetails.create("Google Vision API Logo Annotations",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD), new SchemaProducer());
  }

  private final BQDestination destination;

  /**
   * Creates a processor and specifies the table id to persist to.
   */
  public LogoAnnotationProcessor(String tableId) {
    destination = new BQDestination(tableId);
  }

  @Override
  public Iterable<KV<BQDestination, TableRow>> process(
      String gcsURI, AnnotateImageResponse response) {
    int numberOfAnnotations = response.getLogoAnnotationsCount();
    if (numberOfAnnotations == 0) {
      return null;
    }

    counter.inc(numberOfAnnotations);

    Collection<KV<BQDestination, TableRow>> result = new ArrayList<>(numberOfAnnotations);
    for (EntityAnnotation annotation : response.getLabelAnnotationsList()) {
      TableRow row = ProcessorUtils.startRow(gcsURI);
      row.put(Field.MID_FIELD, annotation.getMid());
      row.put(Field.DESCRIPTION_FIELD, annotation.getDescription());
      row.put(Field.SCORE_FIELD, annotation.getScore());
      ProcessorUtils.extractBoundingPoly(annotation, row);

      LOG.debug("Processing {}", row);
      result.add(KV.of(destination, row));
    }
    return result;
  }
}
