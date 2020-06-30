/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.solutions.ml.api.vision.common;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class BigQueryDynamicTransform
    extends PTransform<PCollection<KV<String, TableRow>>, WriteResult> {
  public static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicTransform.class);

  public abstract String projectId();

  public abstract String datasetId();

  public static Builder newBuilder() {
    return new AutoValue_BigQueryDynamicTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDatasetId(String projectId);

    public abstract Builder setProjectId(String datasetId);

    public abstract BigQueryDynamicTransform build();
  }

  @Override
  public WriteResult expand(PCollection<KV<String, TableRow>> input) {

    return input.apply(
        "BQ Write",
        BigQueryIO.<KV<String, TableRow>>write()
            .to(new BQDestination(datasetId(), projectId()))
            .withFormatFunction(
                element -> {
                  return element.getValue();
                })
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withoutValidation()
            .ignoreInsertIds()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  public class BQDestination
      extends DynamicDestinations<KV<String, TableRow>, KV<String, TableRow>> {

    private String datasetName;
    private String projectId;

    public BQDestination(String datasetName, String projectId) {
      this.datasetName = datasetName;
      this.projectId = projectId;
    }

    @Override
    public KV<String, TableRow> getDestination(ValueInSingleWindow<KV<String, TableRow>> element) {
      String key = element.getValue().getKey();
      String tableName = String.format("%s:%s.%s", projectId, datasetName, key);
      LOG.info("Table Name {}", tableName);
      return KV.of(tableName, element.getValue().getValue());
    }

    @Override
    public TableDestination getTable(KV<String, TableRow> destination) {
      TableDestination dest =
          new TableDestination(destination.getKey(), "vision api data from dataflow");
      LOG.info("Table Destination {}", dest.getTableSpec());
      return dest;
    }

    @Override
    public TableSchema getSchema(KV<String, TableRow> destination) {
      TableSchema schema;
      LOG.info("Key {}", destination.getKey());
      String key = destination.getKey().split("\\.")[1];
      LOG.info("Table schema key {}", key);

      switch (key) {
        case "LABEL_ANNOTATION":
          schema = BigQueryUtils.toTableSchema(Util.labelAnnotationSchema);
          break;
        case "LANDMARK_ANNOTATION":
          schema = BigQueryUtils.toTableSchema(Util.landmarkAnnotationSchema);
          break;
        default:
          schema = BigQueryUtils.toTableSchema(Util.labelAnnotationSchema);
          break;
      }
      LOG.info("Schema {} ", schema.toString());
      return schema;
    }
  }
}
