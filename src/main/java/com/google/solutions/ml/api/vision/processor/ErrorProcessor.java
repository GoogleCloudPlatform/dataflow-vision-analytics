package com.google.solutions.ml.api.vision.processor;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.common.collect.ImmutableList;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.TableDetails;
import com.google.solutions.ml.api.vision.TableSchemaProducer;
import com.google.solutions.ml.api.vision.processor.Constants.Field;
import java.util.Collections;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorProcessor implements AnnotationProcessor {

  private static final long serialVersionUID = 1L;

  public final static Counter counter =
      Metrics.counter(AnnotationProcessor.class, "numberOfErrors");
  public static final Logger LOG = LoggerFactory.getLogger(ErrorProcessor.class);

  private static class SchemaProducer implements TableSchemaProducer {

    private static final long serialVersionUID = 1L;

    @Override
    public TableSchema getTableSchema() {
      return new TableSchema().setFields(
          ImmutableList.of(
              new TableFieldSchema()
                  .setName(Field.GCS_URI_FIELD)
                  .setType("STRING")
                  .setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.DESCRIPTION_FIELD).setType("STRING")
                  .setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.STACK_TRACE).setType("STRING")
                  .setMode("NULLABLE"),
              new TableFieldSchema()
                  .setName(Field.TIMESTAMP_FIELD).setType("TIMESTAMP")
                  .setMode("REQUIRED"))
      );
    }
  }

  @Override
  public TableDetails destinationTableDetails() {
    return new TableDetails("Google Vision API Processing Errors",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD), new SchemaProducer());
  }

  private final BQDestination destination;

  public ErrorProcessor(String tableId) {
    destination = new BQDestination(tableId);
  }

  @Override
  public Iterable<KV<BQDestination, TableRow>> extractAnnotations(
      String gcsURI, AnnotateImageResponse response) {
    if (!response.hasError()) {
      return null;
    }

    counter.inc();

    TableRow result = ProcessorUtils.startRow(gcsURI);
    result.put(Field.DESCRIPTION_FIELD, response.getError().toString());

    LOG.debug("Processing {}", result);

    return Collections.singletonList(KV.of(destination, result));
  }
}
