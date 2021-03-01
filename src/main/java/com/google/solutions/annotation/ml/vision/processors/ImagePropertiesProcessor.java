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
import com.google.cloud.vision.v1.DominantColorsAnnotation;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.ml.Constants.Field;
import com.google.solutions.annotation.bigquery.BigQueryConstants;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.bigquery.TableDetails;
import com.google.solutions.annotation.bigquery.TableSchemaProducer;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import com.google.solutions.annotation.ml.MLApiResponseProcessor;
import com.google.solutions.annotation.ml.ProcessorUtils;
import com.google.type.Color;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extracts image properties (https://cloud.google.com/vision/docs/detecting-properties) */
public class ImagePropertiesProcessor implements MLApiResponseProcessor {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ImagePropertiesProcessor.class);

  public static final Counter counter =
      Metrics.counter(MLApiResponseProcessor.class, "numberOfImagePropertiesAnnotations");

  private final BigQueryDestination destination;
  private final Set<String> metadataKeys;

  /** Creates a processor and specifies the table id to persist to. */
  public ImagePropertiesProcessor(String tableId, Set<String> metadataKeys) {
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
              .setName(Field.DOMINANT_COLORS)
              .setType(BigQueryConstants.Type.RECORD)
              .setMode(BigQueryConstants.Mode.REQUIRED)
              .setFields(
                  ImmutableList.of(
                      new TableFieldSchema()
                          .setName(Field.COLORS)
                          .setType(BigQueryConstants.Type.RECORD)
                          .setMode(BigQueryConstants.Mode.REPEATED)
                          .setFields(
                              ImmutableList.of(
                                  new TableFieldSchema()
                                      .setName(Field.SCORE_FIELD)
                                      .setType(BigQueryConstants.Type.FLOAT)
                                      .setMode(BigQueryConstants.Mode.REQUIRED),
                                  new TableFieldSchema()
                                      .setName(Field.PIXEL_FRACTION)
                                      .setType(BigQueryConstants.Type.FLOAT)
                                      .setMode(BigQueryConstants.Mode.REQUIRED),
                                  new TableFieldSchema()
                                      .setName(Field.COLOR)
                                      .setType(BigQueryConstants.Type.RECORD)
                                      .setMode(BigQueryConstants.Mode.REQUIRED)
                                      .setFields(
                                          ImmutableList.of(
                                              new TableFieldSchema()
                                                  .setName(Field.COLOR_RED)
                                                  .setType(BigQueryConstants.Type.FLOAT)
                                                  .setMode(BigQueryConstants.Mode.REQUIRED),
                                              new TableFieldSchema()
                                                  .setName(Field.COLOR_BLUE)
                                                  .setType(BigQueryConstants.Type.FLOAT)
                                                  .setMode(BigQueryConstants.Mode.REQUIRED),
                                              new TableFieldSchema()
                                                  .setName(Field.COLOR_GREEN)
                                                  .setType(BigQueryConstants.Type.FLOAT)
                                                  .setMode(BigQueryConstants.Mode.REQUIRED),
                                              new TableFieldSchema()
                                                  .setName(Field.COLOR_ALPHA)
                                                  .setType(BigQueryConstants.Type.FLOAT)
                                                  .setMode(BigQueryConstants.Mode.NULLABLE))))))));
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
        "Google Vision API Image Properties",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD),
        new SchemaProducer(metadataKeys));
  }

  @Override
  public boolean shouldProcess(GeneratedMessageV3 response) {
    return (response instanceof AnnotateImageResponse
        && ((AnnotateImageResponse) response).getImagePropertiesAnnotation().hasDominantColors());
  }

  @Override
  public ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 r) {
    AnnotateImageResponse response = (AnnotateImageResponse) r;
    counter.inc();
    TableRow row = ProcessorUtils.startRow(fileInfo);
    DominantColorsAnnotation dominantColors =
        response.getImagePropertiesAnnotation().getDominantColors();
    List<TableRow> colors = new ArrayList<>(dominantColors.getColorsCount());
    dominantColors
        .getColorsList()
        .forEach(
            colorInfo -> {
              TableRow colorInfoRow = new TableRow();
              colorInfoRow.put(Field.SCORE_FIELD, colorInfo.getScore());
              colorInfoRow.put(Field.PIXEL_FRACTION, colorInfo.getPixelFraction());
              Color color = colorInfo.getColor();
              TableRow colorRow = new TableRow();
              colorRow.put(Field.COLOR_RED, color.getRed());
              colorRow.put(Field.COLOR_GREEN, color.getGreen());
              colorRow.put(Field.COLOR_BLUE, color.getBlue());
              if (color.hasAlpha()) {
                colorRow.put(Field.COLOR_ALPHA, color.getAlpha());
              }
              colorInfoRow.put(Field.COLOR, colorRow);
              colors.add(colorInfoRow);
            });
    TableRow colorsRow = new TableRow();
    colorsRow.put(Field.COLORS, colors);
    row.put(Field.DOMINANT_COLORS, colorsRow);

    ProcessorUtils.addMetadataValues(row, fileInfo, metadataKeys);
    LOG.debug("Processing {}", row);

    ProcessorResult result = new ProcessorResult(ProcessorResult.IMAGE_PROPERTIES, destination);
    result.allRows.add(row);

    return result;
  }
}
