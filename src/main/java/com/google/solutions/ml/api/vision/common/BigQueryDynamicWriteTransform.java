/*
 * Copyright 2020 Google LLC
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
package com.google.solutions.ml.api.vision.common;

import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.TableDetails;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoderV3;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public class BigQueryDynamicWriteTransform
    extends PTransform<PCollection<KV<BQDestination, TableRow>>, WriteResult> {

  private static final long serialVersionUID = 1L;
  private final String projectId;
  private final String datasetId;
  private final Map<String, TableDetails> tableNameToTableDetailsMap;

  public BigQueryDynamicWriteTransform(String projectId, String datasetId,
      Map<String, TableDetails> tableNameToTableDetailsMap) {
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.tableNameToTableDetailsMap = tableNameToTableDetailsMap;
  }

  @Override
  public WriteResult expand(PCollection<KV<BQDestination, TableRow>> input) {
    return input.apply(
        "BQ Write",
        BigQueryIO.<KV<BQDestination, TableRow>>write()
            .to(new AnnotationDestination())
            .withFormatFunction(KV::getValue)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withoutValidation()
//            .withMethod(Method.STREAMING_INSERTS)
            .withClustering()
            .ignoreInsertIds()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  public class AnnotationDestination extends
      DynamicDestinations<KV<BQDestination, TableRow>, BQDestination> {

    private final static long serialVersionUID = 1l;

    @Override
    public BQDestination getDestination(
        ValueInSingleWindow<KV<BQDestination, TableRow>> element) {
      return element.getValue().getKey();
    }

    @Override
    public TableDestination getTable(BQDestination destination) {
      TableDetails tableDetails = tableDetails(destination);
      return new TableDestination(
          new TableReference()
              .setProjectId(projectId)
              .setDatasetId(datasetId)
              .setTableId(destination.getTableName()),
          tableDetails.getDescription(),
          tableDetails.getTimePartitioningJson(),
          tableDetails.getClusteringJson());
    }

    @Override
    public TableSchema getSchema(BQDestination destination) {
      return tableDetails(destination).getSchemaProducer().getTableSchema();
    }

    private TableDetails tableDetails(BQDestination destination) {
      TableDetails result = tableNameToTableDetailsMap.get(destination.getTableName());
      if (result == null) {
        throw new RuntimeException("Unable to find schema for table " + destination.getTableName());
      }
      return result;
    }
  }
}
