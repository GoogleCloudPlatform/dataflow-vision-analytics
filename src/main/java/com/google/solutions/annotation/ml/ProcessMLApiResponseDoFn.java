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
package com.google.solutions.annotation.ml;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.util.Collection;
import java.util.Objects;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessImageResponse {@link ProcessMLApiResponseDoFn} class parses the API response for specific
 * annotation and using response builder output the table and table row for BigQuery
 */
@AutoValue
public abstract class ProcessMLApiResponseDoFn
    extends DoFn<KV<GCSFileInfo, GeneratedMessageV3>, KV<BigQueryDestination, TableRow>> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ProcessMLApiResponseDoFn.class);

  abstract Collection<MLApiResponseProcessor> processors();

  abstract TupleTag<KV<BigQueryDestination, TableRow>> allRows();

  abstract TupleTag<KV<String, TableRow>> relevantRows();

  abstract Counter processedFileCounter();

  public static ProcessMLApiResponseDoFn create(
      Collection<MLApiResponseProcessor> processors,
      TupleTag<KV<BigQueryDestination, TableRow>> allRows,
      TupleTag<KV<String, TableRow>> relevantRows) {
    return builder()
        .processors(processors)
        .allRows(allRows)
        .relevantRows(relevantRows)
        .processedFileCounter(Metrics.counter(ProcessMLApiResponseDoFn.class, "processedFiles"))
        .build();
  }

  @ProcessElement
  public void processElement(
      @Element KV<GCSFileInfo, GeneratedMessageV3> element, MultiOutputReceiver out) {
    GCSFileInfo fileInfo = element.getKey();
    GeneratedMessageV3 annotationResponse = element.getValue();

    LOG.debug("Processing annotations for file: {}", Objects.requireNonNull(fileInfo).getUri());
    processedFileCounter().inc();

    processors()
        .forEach(
            processor -> {
              if (processor.shouldProcess(annotationResponse)) {
                MLApiResponseProcessor.ProcessorResult result =
                    processor.process(fileInfo, annotationResponse);
                if (result != null) {
                  result.allRows.forEach(
                      (TableRow row) -> out.get(allRows()).output(KV.of(result.destination, row)));
                  result.relevantRows.forEach(
                      (TableRow row) -> out.get(relevantRows()).output(KV.of(result.type, row)));
                }
              }
            });
  }

  public static Builder builder() {
    return new AutoValue_ProcessMLApiResponseDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder processors(Collection<MLApiResponseProcessor> processors);

    public abstract Builder processedFileCounter(Counter processedFileCounter);

    public abstract Builder allRows(TupleTag<KV<BigQueryDestination, TableRow>> allRows);

    public abstract Builder relevantRows(TupleTag<KV<String, TableRow>> relevantRows);

    public abstract ProcessMLApiResponseDoFn build();
  }
}
