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
package com.google.solutions.annotation.ml.videointelligence.processors;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.videointelligence.v1p3beta1.LabelAnnotation;
import com.google.cloud.videointelligence.v1p3beta1.StreamingAnnotateVideoResponse;
import com.google.cloud.videointelligence.v1p3beta1.StreamingVideoAnnotationResults;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.bigquery.TableDetails;
import com.google.solutions.annotation.bigquery.TableSchemaProducer;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import com.google.solutions.annotation.ml.Constants.Field;
import com.google.solutions.annotation.ml.MLApiResponseProcessor;
import com.google.solutions.annotation.ml.ProcessorUtils;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VideoLabelAnnotationProcessor implements MLApiResponseProcessor {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(VideoLabelAnnotationProcessor.class);
  private final BigQueryDestination destination;
  private final Set<String> metadataKeys;
  private final Set<String> relevantEntities;
  private final float confidenceThreshold;
  public static final Counter counter =
      Metrics.counter(MLApiResponseProcessor.class, "numberOfVideoLabelAnnotations");

  public VideoLabelAnnotationProcessor(
      String tableId,
      Set<String> metadataKeys,
      Set<String> relevantEntities,
      float confidenceThreshold) {
    this.destination = new BigQueryDestination(tableId);
    this.metadataKeys = metadataKeys;
    this.relevantEntities = relevantEntities;
    this.confidenceThreshold = confidenceThreshold;
  }

  private static class SchemaProducer implements TableSchemaProducer {

    private static final long serialVersionUID = 1L;
    private final Set<String> metadataKeys;

    SchemaProducer(Set<String> metadataKeys) {
      this.metadataKeys = metadataKeys;
    }

    /**
     * Note: Only frame level label detection results are emitted by the Streaming API. This is due
     * to streaming's nature. Because in live stream, one cannot predict when a shot change will
     * happen.
     */
    @Override
    public TableSchema getTableSchema() {
      ArrayList<TableFieldSchema> fields = new ArrayList<>();
      fields.add(
          new TableFieldSchema()
              .setName(Field.GCS_URI_FIELD)
              .setType("STRING")
              .setMode("REQUIRED"));
      fields.add(
          new TableFieldSchema()
              .setName(Field.TIMESTAMP_FIELD)
              .setType("TIMESTAMP")
              .setMode("REQUIRED"));
      fields.add(
          new TableFieldSchema().setName(Field.ENTITY).setType("STRING").setMode("REQUIRED"));
      fields.add(
          new TableFieldSchema()
              .setName(Field.FRAMES)
              .setType("RECORD")
              .setMode("REPEATED")
              .setFields(
                  ImmutableList.of(
                      new TableFieldSchema()
                          .setName(Field.CONFIDENCE)
                          .setType("FLOAT")
                          .setMode("REQUIRED"),
                      new TableFieldSchema()
                          .setName(Field.TIME_OFFSET)
                          .setType("INT64")
                          .setMode("REQUIRED"))));

      ProcessorUtils.setMetadataFieldsSchema(fields, metadataKeys);

      return new TableSchema().setFields(fields);
    }
  }

  @Override
  public boolean shouldProcess(GeneratedMessageV3 response) {
    return (response instanceof StreamingAnnotateVideoResponse
        && ((StreamingAnnotateVideoResponse) response)
                .getAnnotationResults()
                .getLabelAnnotationsCount()
            > 0);
  }

  @Override
  public ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 r) {
    StreamingAnnotateVideoResponse response = (StreamingAnnotateVideoResponse) r;
    StreamingVideoAnnotationResults annotationResults = response.getAnnotationResults();
    counter.inc(annotationResults.getLabelAnnotationsCount());
    ProcessorResult result = new ProcessorResult(ProcessorResult.VIDEO_LABEL, destination);
    for (LabelAnnotation annotation : annotationResults.getLabelAnnotationsList()) {
      TableRow row = ProcessorUtils.startRow(fileInfo);
      row.set(
          Field.ENTITY,
          annotation.hasEntity()
              ? annotation.getEntity().getDescription()
              : "NOT_FOUND"); // FIXME: Seems like sometimes it's an empty string?

      List<TableRow> frames = new ArrayList<>(annotation.getFramesCount());
      AtomicReference<Float> maxConfidence = new AtomicReference<>((float) 0);
      annotation
          .getFramesList()
          .forEach(
              frame -> {
                TableRow frameRow = new TableRow();
                frameRow.set(Field.CONFIDENCE, frame.getConfidence());
                frameRow.set(Field.TIME_OFFSET, frame.getTimeOffset().getSeconds());
                frames.add(frameRow);
                maxConfidence.set(Math.max(maxConfidence.get(), frame.getConfidence()));
              });
      row.put(Field.FRAMES, frames);

      ProcessorUtils.addMetadataValues(row, fileInfo, metadataKeys);

      LOG.debug("Processing {}", row);
      result.allRows.add(row);

      if (relevantEntities != null
          && relevantEntities.stream()
              .anyMatch(annotation.getEntity().getDescription()::equalsIgnoreCase)
          && maxConfidence.get() >= confidenceThreshold) {
        result.relevantRows.add(row);
      }
    }
    return result;
  }

  @Override
  public TableDetails destinationTableDetails() {
    return TableDetails.create(
        "Google Video Intelligence API label annotations",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD),
        new SchemaProducer(metadataKeys));
  }
}
