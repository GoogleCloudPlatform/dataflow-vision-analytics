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
import com.google.common.collect.ImmutableSet;
import com.google.solutions.ml.api.vision.common.AnnotateImagesDoFn;
import com.google.solutions.ml.api.vision.common.AnnotateImagesSimulatorDoFn;
import com.google.solutions.ml.api.vision.common.BatchRequestsTransform;
import com.google.solutions.ml.api.vision.common.BigQueryDynamicWriteTransform;
import com.google.solutions.ml.api.vision.common.ProcessImageResponseDoFn;
import com.google.solutions.ml.api.vision.common.PubSubNotificationToGCSUriDoFn;
import com.google.solutions.ml.api.vision.common.Util;
import com.google.solutions.ml.api.vision.processor.AnnotationProcessor;
import com.google.solutions.ml.api.vision.processor.ErrorProcessor;
import com.google.solutions.ml.api.vision.processor.FaceAnnotationProcessor;
import com.google.solutions.ml.api.vision.processor.LabelProcessor;
import com.google.solutions.ml.api.vision.processor.LandmarkProcessor;
import com.google.solutions.ml.api.vision.processor.LogoProcessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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


  private static final Set<String> SUPPORTED_CONTENT_TYPES = ImmutableSet.of(
      "image/jpeg", "image/png", "image/tiff", "image/tif", "image/gif"
  );

  public static final String ACCEPTED_FILE_PATTERN = "(^.*\\.(JPEG|jpeg|JPG|jpg|PNG|png|GIF|gif|TIFF|tiff|TIF|tif)$)";

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link VisionAnalyticsPipeline#run(VisionAnalyticsPipelineOptions)}
   * method to start the pipeline and invoke {@code result.waitUntilFinish()} on the {@link
   * PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

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
  public static PipelineResult run(VisionAnalyticsPipelineOptions options) {
    Pipeline p = Pipeline.create(options);

    boolean isBatchJob;

    PCollection<String> imageFileUris;
    if (options.getSubscriberId() != null) {
      imageFileUris = convertPubSubNotificationsToGCSURIs(p, options);
      isBatchJob = false;
    } else if (options.getFileList() != null) {
      imageFileUris = listGCSFiles(p, options);
      isBatchJob = true;
    } else {
      throw new RuntimeException("Either subscriber id or the file list should be provided.");
    }

//    PCollectionView<Map<String, ImageContext>> imageContext = null;
//    PCollection<List<AnnotateImageResponse>> annotationResponses = filteredImages
//        .apply("Annotate images", CloudVision.annotateImagesFromGcsUri(imageContext,
//            converFeatureTypesToFeatures(options
//                .getFeatures()), options.getBatchSize(), 1));
//
//    annotationResponses
//        .apply("Standard processing", ParDo.of(new DoFn<List<AnnotateImageResponse>, String>() {
//          @ProcessElement
//          public void process(@Element List<AnnotateImageResponse> element,
//              OutputReceiver<String> outputReceiver) {
//            element.forEach(e -> {
//              ImageAnnotationContext context = e.getContext();
//              if (context == null) {
//                LOG.info("Empty context");
//                return;
//              }
//              LOG.info("Context.imageURI: {}", context.getUri());
//            });
//          }
//        }));

    PCollection<Iterable<String>> batchedImageURIs = imageFileUris
        .apply("Batch images",
            BatchRequestsTransform.create(options.getBatchSize(), options.getKeyRange()));

    PCollection<KV<String, AnnotateImageResponse>> annotatedImages =
        options.isSimulate() ?
            batchedImageURIs.apply("Simulate Annotation",
                ParDo.of(new AnnotateImagesSimulatorDoFn(options.getFeatures()))) :
            batchedImageURIs.apply(
                "Annotate Images",
                ParDo.of(new AnnotateImagesDoFn(options.getFeatures())));

    Map<String, AnnotationProcessor> processors = configureProcessors(options);

    PCollection<KV<BQDestination, TableRow>> annotationOutcome =
        annotatedImages.apply(
            "Process Annotations",
            ParDo.of(new ProcessImageResponseDoFn(new ArrayList<>(processors.values()))));

    annotationOutcome.apply("Write To BigQuery", new BigQueryDynamicWriteTransform(
        options.getVisionApiProjectId(), options.getDatasetName(),
        tableNameToTableDetailsMap(processors)));

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

  static Map<String, TableDetails> tableNameToTableDetailsMap(
      Map<String, AnnotationProcessor> processors) {
    Map<String, TableDetails> tableNameToTableDetailsMap = new HashMap<>();
    processors.forEach(
        (tableName, processor) -> tableNameToTableDetailsMap
            .put(tableName, processor.destinationTableDetails()));
    return tableNameToTableDetailsMap;
  }

  static PCollection<String> convertPubSubNotificationsToGCSURIs(
      Pipeline p, VisionAnalyticsPipelineOptions options) {
    PCollection<String> imageFileUris;
    PCollection<PubsubMessage> pubSubNotifications = p.begin().apply("Read PubSub",
        PubsubIO.readMessagesWithAttributes().fromSubscription(options.getSubscriberId()));
    imageFileUris = pubSubNotifications
        .apply("PubSub to GCS URIs",
            ParDo.of(PubSubNotificationToGCSUriDoFn.create(SUPPORTED_CONTENT_TYPES)))
        .apply(
            "Fixed Window",
            Window.<String>into(
                FixedWindows.of(Duration.standardSeconds(options.getWindowInterval())))
                .triggering(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO));
    return imageFileUris;
  }

  static PCollection<String> listGCSFiles(Pipeline p, VisionAnalyticsPipelineOptions options) {
    PCollection<String> imageFileUris;
    PCollection<Metadata> allFiles = p.begin()
        .apply(Create.of(Arrays.asList(options.getFileList().split(","))))
        .apply("List GCS Bucket(s)", FileIO.matchAll());
    imageFileUris = allFiles.apply(ParDo.of(new DoFn<Metadata, String>() {
      private static final long serialVersionUID = 1L;

      @ProcessElement
      public void processElement(@Element Metadata metadata, OutputReceiver<String> out) {
        out.output(metadata.resourceId().toString());
      }
    }))
        .apply("Filter out non-image files",
            Filter.by((SerializableFunction<String, Boolean>) fileName -> {
              totalFiles.inc();
              if (fileName.matches(ACCEPTED_FILE_PATTERN)) {
                return true;
              }
              LOG.warn(Util.NO_VALID_EXT_FOUND_ERROR_MESSAGE, fileName);
              rejectedFiles.inc();
              return false;
            }));
    return imageFileUris;
  }

  private static Map<String, AnnotationProcessor> configureProcessors(
      VisionAnalyticsPipelineOptions options) {
    Map<String, AnnotationProcessor> result = new HashMap<>();

    String tableName = options.getLabelAnnotationTable();
    result.put(tableName, new LabelProcessor(tableName));

    tableName = options.getLandmarkAnnotationTable();
    result.put(tableName, new LandmarkProcessor(tableName));

    tableName = options.getLogoAnnotationTable();
    result.put(tableName, new LogoProcessor(tableName));

    tableName = options.getFaceAnnotationTable();
    result.put(tableName, new FaceAnnotationProcessor(tableName));

    tableName = options.getErrorLogTable();
    result.put(tableName, new ErrorProcessor(tableName));

    return result;
  }

  private static void printInterestingMetrics(PipelineResult pipelineResult) {
    MetricResults metrics = pipelineResult.metrics();
    MetricQueryResults interestingMetrics = metrics.queryMetrics(MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.inNamespace(VisionAnalyticsPipeline.class)).build());
    LOG.info("Pipeline completed. Metrics: {}", interestingMetrics.toString());
  }
}
