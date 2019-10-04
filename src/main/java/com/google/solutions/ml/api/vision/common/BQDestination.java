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

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BQ Destination {@link BQDestination} class take a key value pair of BQ table name and row and
 * automatically detect the schema to create the table.
 */
public class BQDestination extends DynamicDestinations<KV<String, TableRow>, KV<String, TableRow>> {
  public static final Logger LOG = LoggerFactory.getLogger(BQDestination.class);

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
    LOG.debug("Table Name {}", tableName);
    return KV.of(tableName, element.getValue().getValue());
  }

  @Override
  public TableDestination getTable(KV<String, TableRow> destination) {
    TableDestination dest =
        new TableDestination(destination.getKey(), "vision api data from dataflow");
    LOG.debug("Table Destination {}", dest.getTableSpec());
    return dest;
  }

  @Override
  public TableSchema getSchema(KV<String, TableRow> destination) {

    TableRow bqRow = destination.getValue();
    TableSchema schema = new TableSchema();
    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
    List<TableCell> cells = bqRow.getF();
    for (int i = 0; i < cells.size(); i++) {
      Map<String, Object> object = cells.get(i);
      String header = object.keySet().iterator().next();
      /** currently all BQ data types are set to String */
      fields.add(new TableFieldSchema().setName(header).setType("STRING"));
    }

    schema.setFields(fields);
    LOG.info("Schema {}", schema.toString());
    return schema;
  }
}
