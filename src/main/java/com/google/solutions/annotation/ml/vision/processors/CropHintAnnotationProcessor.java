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
import com.google.cloud.vision.v1.CropHint;
import com.google.cloud.vision.v1.CropHintsAnnotation;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.bigquery.BigQueryConstants;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.bigquery.TableDetails;
import com.google.solutions.annotation.bigquery.TableSchemaProducer;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import com.google.solutions.annotation.ml.Constants;
import com.google.solutions.annotation.ml.Constants.Field;
import com.google.solutions.annotation.ml.MLApiResponseProcessor;
import com.google.solutions.annotation.ml.ProcessorUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts crop hint annotations (https://cloud.google.com/vision/docs/detecting-crop-hints)
 *
 * <p>Note: requests for either CROP_HINT feature or IMAGE_PROPERTIES feature will produce crop
 * hints
 */
public class CropHintAnnotationProcessor implements MLApiResponseProcessor {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(CropHintAnnotationProcessor.class);
  public static final Counter counter =
      Metrics.counter(MLApiResponseProcessor.class, "numberOfCropHintAnnotations");

  private final BigQueryDestination destination;
  private final Set<String> metadataKeys;
  private final float confidenceThreshold;

  /** Creates a processor and specifies the table id to persist to. */
  public CropHintAnnotationProcessor(
      String tableId, Set<String> metadataKeys, float confidenceThreshold) {
    this.destination = new BigQueryDestination(tableId);
    this.metadataKeys = metadataKeys;
    this.confidenceThreshold = confidenceThreshold;
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
              .setName(Field.CROP_HINTS)
              .setType(BigQueryConstants.Type.RECORD)
              .setMode(BigQueryConstants.Mode.REPEATED)
              .setFields(
                  ImmutableList.of(
                      new TableFieldSchema()
                          .setName(Field.CONFIDENCE)
                          .setType(BigQueryConstants.Type.FLOAT)
                          .setMode(BigQueryConstants.Mode.REQUIRED),
                      new TableFieldSchema()
                          .setName(Field.IMPORTANCE_FRACTION)
                          .setType(BigQueryConstants.Type.FLOAT)
                          .setMode(BigQueryConstants.Mode.REQUIRED),
                      new TableFieldSchema()
                          .setName(Field.BOUNDING_POLY)
                          .setType(BigQueryConstants.Type.RECORD)
                          .setMode(BigQueryConstants.Mode.REQUIRED)
                          .setFields(Constants.POLYGON_FIELDS))));
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
        "Google Vision API Crop Hint Annotations",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD),
        new SchemaProducer(metadataKeys));
  }

  @Override
  public boolean shouldProcess(GeneratedMessageV3 response) {
    return (response instanceof AnnotateImageResponse
        && ((AnnotateImageResponse) response).getCropHintsAnnotation().getCropHintsCount() > 0);
  }

  @Override
  public ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 r) {
    AnnotateImageResponse response = (AnnotateImageResponse) r;
    CropHintsAnnotation cropHintsAnnotation = response.getCropHintsAnnotation();
    counter.inc();

    List<TableRow> cropHintRows =
        new ArrayList<>(response.getCropHintsAnnotation().getCropHintsCount());
    float maxConfidence = 0;
    for (CropHint cropHint : cropHintsAnnotation.getCropHintsList()) {
      TableRow cropHintRow = new TableRow();
      cropHintRow.put(
          Field.BOUNDING_POLY, ProcessorUtils.getBoundingPolyAsRow(cropHint.getBoundingPoly()));
      cropHintRow.put(Field.CONFIDENCE, cropHint.getConfidence());
      cropHintRow.put(Field.IMPORTANCE_FRACTION, cropHint.getImportanceFraction());
      cropHintRows.add(cropHintRow);
      maxConfidence = Math.max(maxConfidence, cropHint.getConfidence());
    }

    TableRow row = ProcessorUtils.startRow(fileInfo);
    row.put(Field.CROP_HINTS, cropHintRows);
    ProcessorUtils.addMetadataValues(row, fileInfo, metadataKeys);
    LOG.debug("Processing {}", row);

    ProcessorResult result = new ProcessorResult(ProcessorResult.IMAGE_CROP, destination);
    result.allRows.add(row);

    if (maxConfidence >= confidenceThreshold) {
      result.relevantRows.add(row);
    }

    return result;
  }
}
