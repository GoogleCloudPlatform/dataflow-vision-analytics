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
import com.google.solutions.ml.api.vision.common.BigQueryDynamicWriteTransform;
import com.google.solutions.ml.api.vision.common.ProcessImageResponseDoFn;
import com.google.solutions.ml.api.vision.common.PubSubNotificationToGCSUriDoFn;
import com.google.solutions.ml.api.vision.common.Util;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
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
    final Counter totalFiles = Metrics.counter(VisionAnalyticsPipeline.class, "totalFiles");
    final Counter rejectedFiles = Metrics
        .counter(VisionAnalyticsPipeline.class, "rejectedFiles");
    final Counter processedFiles = Metrics
        .counter(VisionAnalyticsPipeline.class, "rejectedFiles");

    Pipeline p = Pipeline.create(options);

    PCollection<String> imageFileUris;
    if (options.getSubscriberId() != null) {
      PCollection<PubsubMessage> pubSubNotifications = p.begin().apply("Read Pub/Sub",
          PubsubIO.readMessagesWithAttributes().fromSubscription(options.getSubscriberId()));
      imageFileUris = pubSubNotifications
          .apply(ParDo.of(new PubSubNotificationToGCSUriDoFn()))
          .apply(
              "Fixed Window",
              Window.<String>into(
                  FixedWindows.of(Duration.standardSeconds(options.getWindowInterval())))
                  .triggering(AfterWatermark.pastEndOfWindow())
                  .discardingFiredPanes()
                  .withAllowedLateness(Duration.ZERO));
    } else if(options.getFileList() != null) {
      PCollection<Metadata> allFiles = p.begin()
          .apply(Create.of(Arrays.asList(options.getFileList().split(","))))
          .apply("List GCS Bucket(s)", FileIO.matchAll());
      imageFileUris = allFiles.apply(ParDo.of(new DoFn<Metadata, String>() {
        @ProcessElement
        public void processElement(@Element Metadata metadata, OutputReceiver<String> out) {
          out.output(metadata.resourceId().toString());
        }
      }));
    } else {
      throw new RuntimeException("Either subscriber id or the file list should be provided.");
    }

    PCollection<String> filteredImages = imageFileUris
        .apply(Filter.by((SerializableFunction<String, Boolean>) fileName -> {
          totalFiles.inc();
          if (fileName.matches(Util.FILE_PATTERN)) {
            return true;
          }
          LOG.warn(Util.NO_VALID_EXT_FOUND_ERROR_MESSAGE, fileName);
          rejectedFiles.inc();
          return false;
        }));

    PCollection<Iterable<String>> batchedImageURIs = filteredImages
        .apply(WithKeys.of("1"))
        .apply(GroupIntoBatches.ofSize(options.getBatchSize()))
        .apply(Values.create());

    PCollection<KV<String, AnnotateImageResponse>> annotatedImages =
        batchedImageURIs.apply(
            "Annotate Images",
            ParDo.of(new AnnotateImagesDoFn(options.getFeatures())));

    PCollection<KV<String, TableRow>> annotationOutcome =
        annotatedImages.apply(
            "Process Response",
            ParDo.of(new ProcessImageResponseDoFn()));

    annotationOutcome.apply(
        "WriteToBq",
        BigQueryDynamicWriteTransform.newBuilder()
            .setDatasetId(options.getDatasetName())
            .setProjectId(options.getVisionApiProjectId())
            .build());

    return p.run();
  }
}
