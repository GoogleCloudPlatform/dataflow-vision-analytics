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

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.CropHint;
import com.google.cloud.vision.v1.CropHintsAnnotation;
import com.google.common.collect.ImmutableList;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.BigQueryConstants.Mode;
import com.google.solutions.ml.api.vision.BigQueryConstants.Type;
import com.google.solutions.ml.api.vision.TableDetails;
import com.google.solutions.ml.api.vision.TableSchemaProducer;
import com.google.solutions.ml.api.vision.processor.Constants.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts crop hint annotations (https://cloud.google.com/vision/docs/detecting-crop-hints)
 *
 * <p>Note: requests for either CROP_HINT feature or IMAGE_PROPERTIES feature will produce crop
 * hints
 */
public class CropHintAnnotationProcessor implements AnnotateImageResponseProcessor {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(CropHintAnnotationProcessor.class);
  public static final Counter counter =
      Metrics.counter(AnnotateImageResponseProcessor.class, "numberOfCropHintAnnotations");

  private static class SchemaProducer implements TableSchemaProducer {

    private static final long serialVersionUID = 1L;

    @Override
    public TableSchema getTableSchema() {
      return new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema()
                      .setName(Field.GCS_URI_FIELD)
                      .setType(Type.STRING)
                      .setMode(Mode.REQUIRED),
                  new TableFieldSchema()
                      .setName(Field.CROP_HINTS)
                      .setType(Type.RECORD)
                      .setMode(Mode.REPEATED)
                      .setFields(
                          ImmutableList.of(
                              new TableFieldSchema()
                                  .setName(Field.CONFIDENCE)
                                  .setType(Type.FLOAT)
                                  .setMode(Mode.REQUIRED),
                              new TableFieldSchema()
                                  .setName(Field.IMPORTANCE_FRACTION)
                                  .setType(Type.FLOAT)
                                  .setMode(Mode.REQUIRED),
                              new TableFieldSchema()
                                  .setName(Field.BOUNDING_POLY)
                                  .setType(Type.RECORD)
                                  .setMode(Mode.REQUIRED)
                                  .setFields(Constants.POLYGON_FIELDS))),
                  new TableFieldSchema()
                      .setName(Field.TIMESTAMP_FIELD)
                      .setType(Type.TIMESTAMP)
                      .setMode(Mode.REQUIRED)));
    }
  }

  @Override
  public TableDetails destinationTableDetails() {
    return TableDetails.create(
        "Google Vision API Crop Hint Annotations",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD),
        new SchemaProducer());
  }

  private final BQDestination destination;

  /** Creates a processor and specifies the table id to persist to. */
  public CropHintAnnotationProcessor(String tableId) {
    destination = new BQDestination(tableId);
  }

  @Override
  public Iterable<KV<BQDestination, TableRow>> process(
      String gcsURI, AnnotateImageResponse response) {
    CropHintsAnnotation cropHintsAnnotation = response.getCropHintsAnnotation();
    if (cropHintsAnnotation == null) {
      return null;
    }
    int cropHintsCount = cropHintsAnnotation.getCropHintsCount();
    if (cropHintsCount == 0) {
      return null;
    }

    counter.inc();

    List<TableRow> cropHintRows = new ArrayList<>(cropHintsCount);
    for (CropHint cropHint : cropHintsAnnotation.getCropHintsList()) {
      TableRow cropHintRow = new TableRow();
      cropHintRow.put(
          Field.BOUNDING_POLY, ProcessorUtils.getBoundingPolyAsRow(cropHint.getBoundingPoly()));
      cropHintRow.put(Field.CONFIDENCE, cropHint.getConfidence());
      cropHintRow.put(Field.IMPORTANCE_FRACTION, cropHint.getImportanceFraction());

      cropHintRows.add(cropHintRow);
    }

    TableRow result = ProcessorUtils.startRow(gcsURI);
    result.put(Field.CROP_HINTS, cropHintRows);
    LOG.debug("Processing {}", result);
    return Collections.singletonList((KV.of(destination, result)));
  }
}
