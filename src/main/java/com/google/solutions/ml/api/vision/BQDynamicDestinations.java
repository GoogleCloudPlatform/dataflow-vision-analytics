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
package com.google.solutions.ml.api.vision;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/** Provides details of the target table. */
@AutoValue
public abstract class BQDynamicDestinations
    extends DynamicDestinations<KV<BQDestination, TableRow>, BQDestination> {

  private static final long serialVersionUID = 1L;

  abstract String projectId();

  abstract String datasetId();

  abstract Map<String, TableDetails> tableNameToTableDetailsMap();

  @Override
  public BQDestination getDestination(ValueInSingleWindow<KV<BQDestination, TableRow>> element) {
    return Objects.requireNonNull(element.getValue()).getKey();
  }

  @Override
  public TableDestination getTable(BQDestination destination) {
    TableDetails tableDetails = tableDetails(destination);
    return new TableDestination(
        new TableReference()
            .setProjectId(projectId())
            .setDatasetId(datasetId())
            .setTableId(destination.getTableName()),
        tableDetails.description(),
        tableDetails.timePartitioningJson(),
        tableDetails.clusteringJson());
  }

  @Override
  public TableSchema getSchema(BQDestination destination) {
    return tableDetails(destination).schemaProducer().getTableSchema();
  }

  private TableDetails tableDetails(BQDestination destination) {
    TableDetails result = tableNameToTableDetailsMap().get(destination.getTableName());
    if (result == null) {
      throw new RuntimeException("Unable to find schema for table " + destination.getTableName());
    }
    return result;
  }

  public static Builder builder() {
    return new AutoValue_BQDynamicDestinations.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder projectId(String projectId);

    public abstract Builder datasetId(String datasetId);

    public abstract Builder tableNameToTableDetailsMap(
        Map<String, TableDetails> tableNameToTableDetailsMap);

    public abstract BQDynamicDestinations build();
  }
}
