package com.google.solutions.ml.api.vision.processor;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.common.collect.ImmutableList;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.TableDetails;
import com.google.solutions.ml.api.vision.TableSchemaProducer;
import com.google.solutions.ml.api.vision.common.Util;
import com.google.solutions.ml.api.vision.processor.Constants.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;

public class LabelProcessor implements AnnotationProcessor {
  private static final long serialVersionUID = 1L;

  public final static Counter counter =
      Metrics.counter(AnnotationProcessor.class, "numberOfLabelAnnotations");

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
                  .setName(Field.MID_FIELD).setType("STRING")
                  .setMode("NULLABLE"),
              new TableFieldSchema()
                  .setName(Field.DESCRIPTION_FIELD).setType("STRING")
                  .setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.SCORE_FIELD).setType("FLOAT")
                  .setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.TOPICALITY_FIELD).setType("FLOAT")
                  .setMode("REQUIRED"),
              new TableFieldSchema()
                  .setName(Field.TIMESTAMP_FIELD).setType("TIMESTAMP")
                  .setMode("REQUIRED"))
      );
    }
  }

  @Override
  public TableDetails destinationTableDetails() {
    return new TableDetails("Google Vision API Label Annotations",
        new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
        new TimePartitioning().setField(Field.TIMESTAMP_FIELD), new SchemaProducer());
  }

  private final BQDestination destination;

  public LabelProcessor(String tableId) {
    destination = new BQDestination(tableId);
  }

  @Override
  public Iterable<KV<BQDestination, TableRow>> extractAnnotations(
      String gcsURI, AnnotateImageResponse response) {
    int numberOfAnnotations = response.getLabelAnnotationsCount();
    if (numberOfAnnotations == 0) {
      return null;
    }

    counter.inc(numberOfAnnotations);

    Collection<KV<BQDestination, TableRow>> result = new ArrayList<>(numberOfAnnotations);
    for (EntityAnnotation annotation : response.getLabelAnnotationsList()) {
      TableRow row = ProcessorUtils.startRow(gcsURI);
      row.put(Field.MID_FIELD, annotation.getMid());
      row.put(Field.DESCRIPTION_FIELD, annotation.getDescription());
      row.put(Field.SCORE_FIELD, annotation.getScore());
      row.put(Field.TOPICALITY_FIELD, annotation.getTopicality());

      Util.LOG.debug("Processing {}", row);
      result.add(KV.of(destination, row));
    }
    return result;
  }

}
