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
package com.google.solutions.annotation.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import com.google.solutions.annotation.ml.Constants;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/** Provides details of the target table. */
@AutoValue
public abstract class BigQueryDynamicDestinations
    extends DynamicDestinations<KV<BigQueryDestination, TableRow>, BigQueryDestination> {

  private static final long serialVersionUID = 1L;

  abstract String project();

  abstract String datasetName();

  abstract Set<String> metadataKeys();

  abstract Map<String, TableDetails> tableNameToTableDetailsMap();

  @Override
  public BigQueryDestination getDestination(
      ValueInSingleWindow<KV<BigQueryDestination, TableRow>> element) {
    return Objects.requireNonNull(element.getValue()).getKey();
  }

  @Override
  public TableDestination getTable(BigQueryDestination destination) {
    TableDetails tableDetails = tableDetails(destination);
    return new TableDestination(
        new TableReference()
            .setProjectId(project())
            .setDatasetId(datasetName())
            .setTableId(destination.getTableName()),
        tableDetails.description(),
        tableDetails.timePartitioningJson(),
        tableDetails.clusteringJson());
  }

  @Override
  public TableSchema getSchema(BigQueryDestination destination) {
    TableSchema schema = tableDetails(destination).schemaProducer().getTableSchema();

    // Add metadata fields, if any.
    TableFieldSchema metadataSchema =
        new TableFieldSchema()
            .setName(Constants.Field.METADATA)
            .setType("RECORD")
            .setMode("REPEATED");
    for (String key : metadataKeys()) {
      metadataSchema.set(key, new TableFieldSchema().setName(key).setType("STRING"));
    }
    schema.set("metadata", metadataSchema);
    return schema;
  }

  private TableDetails tableDetails(BigQueryDestination destination) {
    TableDetails result = tableNameToTableDetailsMap().get(destination.getTableName());
    if (result == null) {
      throw new RuntimeException("Unable to find schema for table " + destination.getTableName());
    }
    return result;
  }

  public static Builder builder() {
    return new AutoValue_BigQueryDynamicDestinations.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder project(String projectId);

    public abstract Builder datasetName(String datasetId);

    public abstract Builder metadataKeys(Set<String> metadataKeys);

    public abstract Builder tableNameToTableDetailsMap(
        Map<String, TableDetails> tableNameToTableDetailsMap);

    public abstract BigQueryDynamicDestinations build();
  }
}
