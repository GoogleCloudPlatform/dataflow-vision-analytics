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
import com.google.cloud.vision.v1.DominantColorsAnnotation;
import com.google.cloud.vision.v1.ImageProperties;
import com.google.common.collect.ImmutableList;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.BigQueryConstants.Mode;
import com.google.solutions.ml.api.vision.BigQueryConstants.Type;
import com.google.solutions.ml.api.vision.TableDetails;
import com.google.solutions.ml.api.vision.TableSchemaProducer;
import com.google.solutions.ml.api.vision.processor.Constants.Field;
import com.google.type.Color;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts image properties (https://cloud.google.com/vision/docs/detecting-properties)
 */
public class ImagePropertiesProcessor implements AnnotateImageResponseProcessor {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(ImagePropertiesProcessor.class);

  public final static Counter counter =
      Metrics.counter(AnnotateImageResponseProcessor.class, "numberOfImagePropertiesAnnotations");

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
                  .setName(Field.DOMINANT_COLORS).setType(Type.RECORD)
                  .setMode(Mode.REQUIRED)
                  .setFields(ImmutableList.of(
                      new TableFieldSchema()
                          .setName(Field.COLORS).setType(Type.RECORD)
                          .setMode(Mode.REPEATED)
                          .setFields(ImmutableList.of(
                              new TableFieldSchema()
                                  .setName(Field.SCORE_FIELD)
                                  .setType(Type.FLOAT)
                                  .setMode(Mode.REQUIRED),
                              new TableFieldSchema()
                                  .setName(Field.PIXEL_FRACTION)
                                  .setType(Type.FLOAT)
                                  .setMode(Mode.REQUIRED),
                              new TableFieldSchema()
                                  .setName(Field.COLOR)
                                  .setType(Type.RECORD)
                                  .setMode(Mode.REQUIRED)
                                  .setFields(ImmutableList.of(
                                      new TableFieldSchema()
                                          .setName(Field.COLOR_RED)
                                          .setType(Type.FLOAT)
                                          .setMode(Mode.REQUIRED),
                                      new TableFieldSchema()
                                          .setName(Field.COLOR_BLUE)
                                          .setType(Type.FLOAT)
                                          .setMode(Mode.REQUIRED),
                                      new TableFieldSchema()
                                          .setName(Field.COLOR_GREEN)
                                          .setType(Type.FLOAT)
                                          .setMode(Mode.REQUIRED),
                                      new TableFieldSchema()
                                          .setName(Field.COLOR_ALPHA)
                                          .setType(Type.FLOAT)
                                          .setMode(Mode.NULLABLE)
                                  ))
                          ))
                  )),
              new TableFieldSchema()
                  .setName(Field.TIMESTAMP_FIELD).setType(Type.TIMESTAMP)
                  .setMode(Mode.REQUIRED))
      );
    }
  }

  @Override
  public TableDetails destinationTableDetails() {
    return TableDetails.create("Google Vision API Image Properties",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD), new SchemaProducer());
  }

  private final BQDestination destination;

  /**
   * Creates a processor and specifies the table id to persist to.
   */
  public ImagePropertiesProcessor(String tableId) {
    destination = new BQDestination(tableId);
  }

  @Override
  public Iterable<KV<BQDestination, TableRow>> process(
      String gcsURI, AnnotateImageResponse response) {
    ImageProperties imageProperties = response.getImagePropertiesAnnotation();
    if (imageProperties == null || !imageProperties.hasDominantColors()) {
      return null;
    }

    counter.inc();

    TableRow result = ProcessorUtils.startRow(gcsURI);

    DominantColorsAnnotation dominantColors = imageProperties.getDominantColors();
    List<TableRow> colors = new ArrayList<>(dominantColors.getColorsCount());
    dominantColors.getColorsList().forEach(
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
        }
    );
    TableRow colorsRow = new TableRow();
    colorsRow.put(Field.COLORS, colors);
    result.put(Field.DOMINANT_COLORS, colorsRow);

    LOG.debug("Processing {}", result);

    return Collections.singletonList(KV.of(destination, result));
  }
}
