package com.google.solutions.ml.api.vision;

import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;

public interface TableSchemaProducer extends Serializable {
  TableSchema getTableSchema();
}
