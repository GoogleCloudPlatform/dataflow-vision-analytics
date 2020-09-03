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


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.solutions.ml.api.vision.processor.AnnotateImageResponseProcessor;
import com.google.solutions.ml.api.vision.processor.CropHintAnnotationProcessor;
import com.google.solutions.ml.api.vision.processor.ErrorProcessor;
import com.google.solutions.ml.api.vision.processor.FaceAnnotationProcessor;
import com.google.solutions.ml.api.vision.processor.ImagePropertiesProcessor;
import com.google.solutions.ml.api.vision.processor.LabelAnnotationProcessor;
import com.google.solutions.ml.api.vision.processor.LandmarkAnnotationProcessor;
import com.google.solutions.ml.api.vision.processor.LogoAnnotationProcessor;
import com.google.solutions.ml.api.vision.processor.ProcessorUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for the vision analytics processing.
 */
public class VisionAnalyticsPipeline {

  public static final Logger LOG = LoggerFactory.getLogger(VisionAnalyticsPipeline.class);

  public static final Counter totalFiles = Metrics
      .counter(VisionAnalyticsPipeline.class, "totalFiles");
  public static final Counter rejectedFiles = Metrics
      .counter(VisionAnalyticsPipeline.class, "rejectedFiles");
  public static final Counter numberOfRequests = Metrics
      .counter(VisionAnalyticsPipeline.class, "numberOfRequests");
  public static final Counter numberOfQuotaExceededRequests = Metrics
      .counter(VisionAnalyticsPipeline.class, "numberOfQuotaExceededRequests");

  public static final Distribution batchSizeDistribution = Metrics
      .distribution(VisionAnalyticsPipeline.class, "batchSizeDistribution");


  private static final Set<String> SUPPORTED_CONTENT_TYPES = ImmutableSet.of(
      "image/jpeg", "image/png", "image/tiff", "image/tif", "image/gif"
  );

  private static final String ACCEPTED_FILE_PATTERN = "(^.*\\.(JPEG|jpeg|JPG|jpg|PNG|png|GIF|gif|TIFF|tiff|TIF|tif)$)";

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

    PCollection<String> imageFileUris;
    if (options.getSubscriberId() != null) {
      imageFileUris = convertPubSubNotificationsToGCSURIs(p, options);
    } else if (options.getFileList() != null) {
      imageFileUris = listGCSFiles(p, options);
    } else {
      throw new RuntimeException("Either the subscriber id or the file list should be provided.");
    }

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

    Map<String, AnnotateImageResponseProcessor> processors = configureProcessors(options);

    PCollection<KV<BQDestination, TableRow>> annotationOutcome =
        annotatedImages.apply(
            "Process Annotations",
            ParDo.of(ProcessImageResponseDoFn.create(ImmutableSet.copyOf(processors.values()))));

    annotationOutcome.apply("Write To BigQuery", new BigQueryDynamicWriteTransform(
        BQDynamicDestinations.builder()
            .projectId(options.getVisionApiProjectId())
            .datasetId(options.getDatasetName())
            .tableNameToTableDetailsMap(
                tableNameToTableDetailsMap(processors)).build())
    );

    collectBatchStatistics(batchedImageURIs, options);

    return p.run();
  }

  /**
   * Collect the statistics on batching the requests. The results are published to a metric. If
   * {@link VisionAnalyticsPipelineOptions#isCollectBatchData()} is true the batch data is saved to
   * BigQuery table "batch_info".
   */
  static void collectBatchStatistics(PCollection<Iterable<String>> batchedImageURIs,
      VisionAnalyticsPipelineOptions options) {

    PCollection<TableRow> batchInfo = batchedImageURIs
        .apply("Collect Batch Stats", ParDo.of(new DoFn<Iterable<String>, TableRow>() {
          private static final long serialVersionUID = 1L;

          @ProcessElement
          public void processElement(@Element Iterable<String> element, BoundedWindow window,
              OutputReceiver<TableRow> out) {
            int size = Iterables.size(element);
            batchSizeDistribution.update(size);
            if (options.isCollectBatchData()) {
              TableRow row = new TableRow();
              row.put("window", window.toString());
              row.put("timestamp", ProcessorUtils.getTimeStamp());
              row.put("size", size);
              List<String> items = new ArrayList<>();
              element.forEach(items::add);
              row.put("items", items);

              out.output(row);
            }
          }
        }));
    if (!options.isCollectBatchData()) {
      return;
    }
    batchInfo.apply(
        BigQueryIO.writeTableRows()
            .to(new TableReference().setProjectId(options.getVisionApiProjectId())
                .setDatasetId(options.getDatasetName()).setTableId("batch_info"))
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withoutValidation()
            .withClustering()
            .ignoreInsertIds()
            .withSchema(new TableSchema().setFields(ImmutableList.of(
                new TableFieldSchema().setName("window").setType("STRING"),
                new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"),
                new TableFieldSchema().setName("size").setType("NUMERIC"),
                new TableFieldSchema().setName("items").setType("STRING").setMode("REPEATED")
            )))
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  /**
   * Create a map of the table details. Each processor will produce <code>TableRow</code>s destined
   * to a different table. Each processor will provide the details about that table.
   *
   * @return map of table details keyed by table name
   */
  static Map<String, TableDetails> tableNameToTableDetailsMap(
      Map<String, AnnotateImageResponseProcessor> processors) {
    Map<String, TableDetails> tableNameToTableDetailsMap = new HashMap<>();
    processors.forEach(
        (tableName, processor) -> tableNameToTableDetailsMap
            .put(tableName, processor.destinationTableDetails()));
    return tableNameToTableDetailsMap;
  }

  /**
   * Reads PubSub messages from the subscription provided by {@link VisionAnalyticsPipelineOptions#getSubscriberId()}.
   *
   * The messages are expected to confirm to the GCS notification message format defined in
   * https://cloud.google.com/storage/docs/pubsub-notifications
   *
   * Notifications are filtered to have one of the supported content types: {@link
   * VisionAnalyticsPipeline#SUPPORTED_CONTENT_TYPES}.
   *
   * @return PCollection of GCS URIs
   */
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
                .withAllowedLateness(Duration.standardMinutes(15)));
    return imageFileUris;
  }

  /**
   * Reads the GCS buckets provided by {@link VisionAnalyticsPipelineOptions#getFileList()}.
   *
   * The file list can contain multiple entries. Each entry can contain wildcards supported by
   * {@link FileIO#matchAll()}.
   *
   * Files are filtered based on their suffixes as defined in {@link VisionAnalyticsPipeline#ACCEPTED_FILE_PATTERN}.
   *
   * @return PCollection of GCS URIs
   */
  static PCollection<String> listGCSFiles(Pipeline p, VisionAnalyticsPipelineOptions options) {
    PCollection<String> imageFileUris;
    PCollection<Metadata> allFiles = p.begin()
        .apply("Get File List", Create.of(options.getFileList()))
        .apply("Match GCS Files", FileIO.matchAll());
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
              LOG.warn("File {} does not contain a valid extension", fileName);
              rejectedFiles.inc();
              return false;
            }));
    return imageFileUris;
  }

  /**
   * Creates a map of well-known {@link AnnotateImageResponseProcessor}s.
   *
   * If additional processors are needed they should be configured in this method.
   */
  private static Map<String, AnnotateImageResponseProcessor> configureProcessors(
      VisionAnalyticsPipelineOptions options) {
    Map<String, AnnotateImageResponseProcessor> result = new HashMap<>();

    String tableName = options.getLabelAnnotationTable();
    result.put(tableName, new LabelAnnotationProcessor(tableName));

    tableName = options.getLandmarkAnnotationTable();
    result.put(tableName, new LandmarkAnnotationProcessor(tableName));

    tableName = options.getLogoAnnotationTable();
    result.put(tableName, new LogoAnnotationProcessor(tableName));

    tableName = options.getFaceAnnotationTable();
    result.put(tableName, new FaceAnnotationProcessor(tableName));

    tableName = options.getImagePropertiesTable();
    result.put(tableName, new ImagePropertiesProcessor(tableName));

    tableName = options.getCropHintAnnotationTable();
    result.put(tableName, new CropHintAnnotationProcessor(tableName));

    tableName = options.getErrorLogTable();
    result.put(tableName, new ErrorProcessor(tableName));

    return result;
  }
}
