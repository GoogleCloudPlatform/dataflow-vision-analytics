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
package com.google.solutions.annotation.ml;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.videointelligence.v1p3beta1.LabelAnnotation;
import com.google.cloud.videointelligence.v1p3beta1.StreamingAnnotateVideoResponse;
import com.google.cloud.videointelligence.v1p3beta1.StreamingVideoAnnotationResults;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.bigquery.TableDetails;
import com.google.solutions.annotation.bigquery.TableSchemaProducer;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.util.ArrayList;
import java.util.Set;

public class FakeProcessor implements MLApiResponseProcessor {

  private final BigQueryDestination destination;
  private final Set<String> metadataKeys;

  public FakeProcessor(String tableId, Set<String> metadataKeys) {
    this.destination = new BigQueryDestination(tableId);
    this.metadataKeys = metadataKeys;
  }

  @Override
  public ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 r) {
    StreamingAnnotateVideoResponse response = (StreamingAnnotateVideoResponse) r;
    StreamingVideoAnnotationResults annotationResults = response.getAnnotationResults();
    ProcessorResult result = new ProcessorResult("fake_type", destination);
    for (LabelAnnotation annotation : annotationResults.getLabelAnnotationsList()) {
      TableRow row = new TableRow();
      row.put(Constants.Field.GCS_URI_FIELD, fileInfo.getUri());
      row.set(Constants.Field.GCS_URI_FIELD, fileInfo.getUri());
      row.set(Constants.Field.ENTITY, annotation.getEntity().getDescription());
      ProcessorUtils.addMetadataValues(row, fileInfo, metadataKeys);

      result.allRows.add(row);

      if (annotation.getEntity().getDescription().equals("chocolate")) {
        result.relevantRows.add(row);
      }
    }
    return result;
  }

  @Override
  public TableDetails destinationTableDetails() {
    return TableDetails.create("", null, null, new SchemaProducer(metadataKeys));
  }

  @Override
  public boolean shouldProcess(GeneratedMessageV3 response) {
    return (response instanceof StreamingAnnotateVideoResponse);
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
              .setType("STRING")
              .setMode("REQUIRED"));
      fields.add(
          new TableFieldSchema()
              .setName(Constants.Field.ENTITY)
              .setType("STRING")
              .setMode("REQUIRED"));

      ProcessorUtils.setMetadataFieldsSchema(fields, metadataKeys);

      return new TableSchema().setFields(fields);
    }
  }
}
