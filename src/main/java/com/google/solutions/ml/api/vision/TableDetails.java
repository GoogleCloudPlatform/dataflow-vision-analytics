package com.google.solutions.ml.api.vision;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;

public class TableDetails implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String description;
  private final String clusteringJson;
  private final String timePartitioningJson;
  private final TableSchemaProducer schemaProducer;

  public TableDetails(String description, Clustering clustering,
      TimePartitioning timePartitioning, TableSchemaProducer schemaProducer) {
    this.description = description;
    // Conversion to String is done because Clustering and TimePartitioning are not serializable
    this.clusteringJson = clustering == null ? null : BigQueryHelpers.toJsonString(clustering);
    this.timePartitioningJson =
        timePartitioning == null ? null : BigQueryHelpers.toJsonString(timePartitioning);
    this.schemaProducer = schemaProducer;
  }

  public String getDescription() {
    return description;
  }

  public String getClusteringJson() {
    return clusteringJson;
  }

  public String getTimePartitioningJson() {
    return timePartitioningJson;
  }

  public TableSchemaProducer getSchemaProducer() {
    return schemaProducer;
  }
}
