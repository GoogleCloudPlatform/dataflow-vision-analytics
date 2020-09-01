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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.solutions.ml.api.vision.processor.AnnotateImageResponseProcessor;
import java.util.Collection;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessImageResponse {@link ProcessImageResponseDoFn} class parses the image response for
 * specific annotation and using image response builder output the table and table row for BigQuery
 */
@AutoValue
abstract public class ProcessImageResponseDoFn
    extends DoFn<KV<String, AnnotateImageResponse>, KV<BQDestination, TableRow>> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ProcessImageResponseDoFn.class);

  abstract Collection<AnnotateImageResponseProcessor> processors();

  abstract Counter processedFileCounter();

  public static ProcessImageResponseDoFn create(
      Collection<AnnotateImageResponseProcessor> processors) {
    return builder()
        .processors(processors)
        .processedFileCounter(Metrics
            .counter(ProcessImageResponseDoFn.class, "processedFiles"))
        .build();
  }

  @ProcessElement
  public void processElement(@Element KV<String, AnnotateImageResponse> element,
      OutputReceiver<KV<BQDestination, TableRow>> out) {
    String imageFileURI = element.getKey();
    AnnotateImageResponse annotationResponse = element.getValue();

    LOG.debug("Processing annotations for file: {}", imageFileURI);
    processedFileCounter().inc();

    processors().forEach(processor -> {
      Iterable<KV<BQDestination, TableRow>> processingResult = processor
          .process(imageFileURI, annotationResponse);
      if (processingResult != null) {
        processingResult.forEach(out::output);
      }
    });
  }

  public static Builder builder() {
    return new AutoValue_ProcessImageResponseDoFn.Builder();
  }


  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder processors(Collection<AnnotateImageResponseProcessor> processors);

    public abstract Builder processedFileCounter(Counter processedFileCounter);

    public abstract ProcessImageResponseDoFn build();
  }
}
