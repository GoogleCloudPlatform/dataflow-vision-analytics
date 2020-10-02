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

package com.google.solutions.ml.api.vision;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;

@AutoValue
public abstract class TableDetails implements Serializable {

  private static final long serialVersionUID = 1L;

  public abstract String description();

  public abstract String clusteringJson();

  public abstract String timePartitioningJson();

  public abstract TableSchemaProducer schemaProducer();

  public static TableDetails create(String description, Clustering clustering,
      TimePartitioning timePartitioning, TableSchemaProducer schemaProducer) {
    return builder()
        .description(description)
        .clusteringJson(clustering == null ? null : BigQueryHelpers.toJsonString(clustering))
        .timePartitioningJson(
            timePartitioning == null ? null : BigQueryHelpers.toJsonString(timePartitioning))
        .schemaProducer(schemaProducer)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_TableDetails.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder description(String description);

    public abstract Builder clusteringJson(String clusteringJson);

    public abstract Builder timePartitioningJson(String timePartitioningJson);

    public abstract Builder schemaProducer(TableSchemaProducer schemaProducer);

    public abstract TableDetails build();
  }

}
