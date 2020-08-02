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
package com.google.solutions.ml.api.vision;


import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.solutions.ml.api.vision.common.AnnotateImagesDoFn;
import com.google.solutions.ml.api.vision.common.BatchRequestsTransform;
import com.google.solutions.ml.api.vision.common.BigQueryDynamicWriteTransform;
import com.google.solutions.ml.api.vision.common.ProcessImageResponseDoFn;
import com.google.solutions.ml.api.vision.common.PubSubNotificationToGCSUriDoFn;
import com.google.solutions.ml.api.vision.common.Util;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VisionAnalyticsPipeline {

  public static final Logger LOG = LoggerFactory.getLogger(VisionAnalyticsPipeline.class);

  public static final Counter totalFiles = Metrics
      .counter(VisionAnalyticsPipeline.class, "totalFiles");
  public static final Counter rejectedFiles = Metrics
      .counter(VisionAnalyticsPipeline.class, "rejectedFiles");
  public static final Counter processedFiles = Metrics
      .counter(VisionAnalyticsPipeline.class, "processedFiles");

  public static final Distribution batchSizeDistribution = Metrics
      .distribution(VisionAnalyticsPipeline.class, "batchSizeDistribution");

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link VisionAnalyticsPipeline#run(VisionAnalyticsPipelineOptions)}
   * method to start the pipeline and invoke {@code result.waitUntilFinish()} on the {@link
   * PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) throws Exception {

    VisionAnalyticsPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(VisionAnalyticsPipelineOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline
   *
   * @return result
   */
  public static PipelineResult run(VisionAnalyticsPipelineOptions options) throws Exception {
    Pipeline p = Pipeline.create(options);

    boolean isBatchJob;

    PCollection<String> imageFileUris;
    if (options.getSubscriberId() != null) {
      PCollection<PubsubMessage> pubSubNotifications = p.begin().apply("Read PubSub",
          PubsubIO.readMessagesWithAttributes().fromSubscription(options.getSubscriberId()));
      imageFileUris = pubSubNotifications
          .apply("PubSub to GCS URIs", ParDo.of(new PubSubNotificationToGCSUriDoFn()))
          .apply(
              "Fixed Window",
              Window.<String>into(
                  FixedWindows.of(Duration.standardSeconds(options.getWindowInterval())))
                  .triggering(AfterWatermark.pastEndOfWindow())
                  .discardingFiredPanes()
                  .withAllowedLateness(Duration.ZERO));
      isBatchJob = false;
    } else if (options.getFileList() != null) {
      PCollection<Metadata> allFiles = p.begin()
          .apply(Create.of(Arrays.asList(options.getFileList().split(","))))
          .apply("List GCS Bucket(s)", FileIO.matchAll());
      imageFileUris = allFiles.apply(ParDo.of(new DoFn<Metadata, String>() {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(@Element Metadata metadata, OutputReceiver<String> out) {
          out.output(metadata.resourceId().toString());
        }
      }));
      isBatchJob = true;
    } else {
      throw new RuntimeException("Either subscriber id or the file list should be provided.");
    }

    PCollection<String> filteredImages = imageFileUris
        .apply("Filter out non-image files",
            Filter.by((SerializableFunction<String, Boolean>) fileName -> {
              totalFiles.inc();
              if (fileName.matches(Util.FILE_PATTERN)) {
                return true;
              }
              LOG.warn(Util.NO_VALID_EXT_FOUND_ERROR_MESSAGE, fileName);
              rejectedFiles.inc();
              return false;
            }));

    PCollection<Iterable<String>> batchedImageURIs = filteredImages
        .apply("Batch images", BatchRequestsTransform.create(options.getBatchSize()));

    PCollection<KV<String, AnnotateImageResponse>> annotatedImages =
        batchedImageURIs.apply(
            "Annotate Images",
            ParDo.of(new AnnotateImagesDoFn(options.getFeatures())));

    PCollection<KV<String, TableRow>> annotationOutcome =
        annotatedImages.apply(
            "Process Annotations",
            ParDo.of(new ProcessImageResponseDoFn()));

    annotationOutcome.apply(
        "Write To BigQuery",
        BigQueryDynamicWriteTransform.newBuilder()
            .setDatasetId(options.getDatasetName())
            .setProjectId(options.getVisionApiProjectId())
            .build());

    batchedImageURIs.apply("Collect Batch Stats", ParDo.of(new DoFn<Iterable<String>, Boolean>() {
      private static final long serialVersionUID = 1L;

      @ProcessElement
      public void processElement(@Element Iterable<String> element) {
        int[] numberOfElementsInTheBatch = new int[]{0};
        element.forEach(x -> numberOfElementsInTheBatch[0]++);
        batchSizeDistribution.update(numberOfElementsInTheBatch[0]);
      }
    }));
    PipelineResult pipelineResult = p.run();

    if (isBatchJob && pipelineResult.getState() == State.DONE) {
      printInterestingMetrics(pipelineResult);
    }

    return pipelineResult;
  }

  private static void printInterestingMetrics(PipelineResult pipelineResult) {
    MetricResults metrics = pipelineResult.metrics();
    MetricQueryResults interestingMetrics = metrics.queryMetrics(MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.inNamespace(VisionAnalyticsPipeline.class)).build());
    LOG.info("Pipeline completed. Metrics: {}", interestingMetrics.toString());
  }
}
