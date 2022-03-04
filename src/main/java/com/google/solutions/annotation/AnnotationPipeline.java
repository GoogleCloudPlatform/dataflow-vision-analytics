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
package com.google.solutions.annotation;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.bigquery.*;
import com.google.solutions.annotation.bigquery.BigQueryConstants.Mode;
import com.google.solutions.annotation.bigquery.BigQueryConstants.Type;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import com.google.solutions.annotation.gcs.GCSUtils;
import com.google.solutions.annotation.ml.MLApiResponseProcessor;
import com.google.solutions.annotation.ml.ProcessMLApiResponseDoFn;
import com.google.solutions.annotation.ml.ProcessorUtils;
import com.google.solutions.annotation.ml.videointelligence.processors.VideoLabelAnnotationProcessor;
import com.google.solutions.annotation.ml.videointelligence.processors.VideoObjectTrackingAnnotationProcessor;
import com.google.solutions.annotation.ml.vision.processors.CropHintAnnotationProcessor;
import com.google.solutions.annotation.ml.vision.processors.ErrorProcessor;
import com.google.solutions.annotation.ml.vision.processors.FaceAnnotationProcessor;
import com.google.solutions.annotation.ml.vision.processors.ImagePropertiesProcessor;
import com.google.solutions.annotation.ml.vision.processors.LabelAnnotationProcessor;
import com.google.solutions.annotation.ml.vision.processors.LandmarkAnnotationProcessor;
import com.google.solutions.annotation.ml.vision.processors.LogoAnnotationProcessor;
import com.google.solutions.annotation.pubsub.PubSubNotificationToGCSInfoDoFn;
import com.google.solutions.annotation.pubsub.WriteRelevantAnnotationsToPubSubTransform;
import java.io.IOException;
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
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnnotationPipeline {

  public static final Logger LOG = LoggerFactory.getLogger(AnnotationPipeline.class);

  public static final Counter totalFiles = Metrics.counter(AnnotationPipeline.class, "totalFiles");
  public static final Counter rejectedFiles =
      Metrics.counter(AnnotationPipeline.class, "rejectedFiles");
  public static final Counter numberOfImageApiRequests =
      Metrics.counter(AnnotationPipeline.class, "numberOfImageApiRequests");
  public static final Counter numberOfVideoApiRequests =
      Metrics.counter(AnnotationPipeline.class, "numberOfVideoApiRequests");
  public static final Counter numberOfQuotaExceededRequests =
      Metrics.counter(AnnotationPipeline.class, "numberOfQuotaExceededRequests");

  public static final Distribution batchSizeDistribution =
      Metrics.distribution(AnnotationPipeline.class, "batchSizeDistribution");

  public static final Set<String> SUPPORTED_IMAGE_CONTENT_TYPES =
      ImmutableSet.of("image/jpeg", "image/png", "image/tiff", "image/tif", "image/gif");

  public static final Set<String> SUPPORTED_VIDEO_CONTENT_TYPES =
      ImmutableSet.of("video/mov", "video/mpeg4", "video/mp4", "video/avi");

  public static final Set<String> SUPPORTED_CONTENT_TYPES =
      Sets.union(SUPPORTED_IMAGE_CONTENT_TYPES, SUPPORTED_VIDEO_CONTENT_TYPES).immutableCopy();

  public static final String ACCEPTED_IMAGE_FILE_PATTERN = "jpeg|jpg|png|gif|tiff|tif";

  public static final String ACCEPTED_VIDEO_FILE_PATTERN = "mp4|mov|mpeg4|avi";

  public static final String ACCEPTED_FILE_PATTERN =
      ACCEPTED_IMAGE_FILE_PATTERN + "|" + ACCEPTED_VIDEO_FILE_PATTERN;

  private static final TupleTag<KV<BigQueryDestination, TableRow>> allRows =
      new TupleTag<KV<BigQueryDestination, TableRow>>() {};
  private static final TupleTag<KV<String, TableRow>> relevantRows =
      new TupleTag<KV<String, TableRow>>() {};

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * AnnotationPipeline#run(AnnotationPipelineOptions)} method to start the pipeline and invoke
   * {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) throws IOException {

    AnnotationPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AnnotationPipelineOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline
   *
   * @return result
   */
  public static PipelineResult run(AnnotationPipelineOptions options) throws IOException {
    Pipeline p = Pipeline.create(options);

    PCollection<GCSFileInfo> fileInfos;
    if (options.getInputNotificationSubscription() != null) {
      fileInfos = convertPubSubNotificationsToGCSFileInfos(p, options);
    } else if (options.getFileList() != null) {
      fileInfos = listGCSFiles(p, options);
    } else {
      throw new RuntimeException("Either the subscriber id or the file list should be provided.");
    }

    PCollection<Iterable<GCSFileInfo>> batchedFileInfos =
        fileInfos.apply(
            "Batch files",
            BatchRequestsTransform.create(options.getBatchSize(), options.getKeyRange()));

    PCollection<KV<GCSFileInfo, GeneratedMessageV3>> annotatedFiles =
        options.isSimulate()
            ? batchedFileInfos.apply(
                "Simulate Annotation",
                ParDo.of(
                    new AnnotateFilesSimulatorDoFn(
                        options.getImageFeatures(), options.getVideoFeatures())))
            : batchedFileInfos.apply(
                "Annotate files",
                ParDo.of(
                    new AnnotateFilesDoFn(options.getImageFeatures(), options.getVideoFeatures())));

    Map<String, MLApiResponseProcessor> processors = configureProcessors(options);

    PCollectionTuple annotationOutcome =
        annotatedFiles.apply(
            "Process Annotations",
            ParDo.of(
                    ProcessMLApiResponseDoFn.create(
                        ImmutableSet.copyOf(processors.values()), allRows, relevantRows))
                .withOutputTags(allRows, TupleTagList.of(relevantRows)));

    annotationOutcome
        .get(allRows)
        .apply(
            "Write All Annotations To BigQuery",
            new BigQueryDynamicWriteTransform(
                BigQueryDynamicDestinations.builder()
                    .project(options.getProject())
                    .datasetName(options.getDatasetName())
                    .metadataKeys(options.getMetadataKeys())
                    .tableNameToTableDetailsMap(tableNameToTableDetailsMap(processors))
                    .build()));

    annotationOutcome
        .get(relevantRows)
        .apply(
            WriteRelevantAnnotationsToPubSubTransform.newBuilder()
                .setTopicId(options.getRelevantAnnotationOutputTopic())
                .build());

    collectBatchStatistics(batchedFileInfos, options);

    return p.run();
  }

  /**
   * Collect the statistics on batching the requests. The results are published to a metric. If
   * {@link AnnotationPipelineOptions#isCollectBatchData()} is true the batch data is saved to
   * BigQuery table "batch_info".
   */
  static void collectBatchStatistics(
      PCollection<Iterable<GCSFileInfo>> batchedFileInfos, AnnotationPipelineOptions options) {

    PCollection<TableRow> batchInfo =
        batchedFileInfos.apply(
            "Collect Batch Stats",
            ParDo.of(
                new DoFn<Iterable<GCSFileInfo>, TableRow>() {
                  private static final long serialVersionUID = 1L;

                  @ProcessElement
                  public void processElement(
                      @Element Iterable<GCSFileInfo> fileInfos,
                      BoundedWindow window,
                      OutputReceiver<TableRow> out,
                      ProcessContext context) {
                    int size = Iterables.size(fileInfos);
                    batchSizeDistribution.update(size);
                    if (context
                        .getPipelineOptions()
                        .as(AnnotationPipelineOptions.class)
                        .isCollectBatchData()) {
                      TableRow row = new TableRow();
                      row.put("window", window.toString());
                      row.put("timestamp", ProcessorUtils.getTimeStamp());
                      row.put("size", size);
                      List<String> fileUris = new ArrayList<>();
                      fileInfos.forEach(
                          (fileInfo) -> {
                            fileUris.add(fileInfo.getUri());
                          });
                      row.put("items", fileUris);
                      out.output(row);
                    }
                  }
                }));
    if (!options.isCollectBatchData()) {
      return;
    }
    batchInfo.apply(
        BigQueryIO.writeTableRows()
            .to(
                new TableReference()
                    .setProjectId(options.getProject())
                    .setDatasetId(options.getDatasetName())
                    .setTableId("batch_info"))
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withoutValidation()
            .withClustering()
            .ignoreInsertIds()
            .withSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("window").setType(Type.STRING),
                            new TableFieldSchema().setName("timestamp").setType(Type.TIMESTAMP),
                            new TableFieldSchema().setName("size").setType(Type.NUMERIC),
                            new TableFieldSchema()
                                .setName("items")
                                .setType(Type.STRING)
                                .setMode(Mode.REPEATED))))
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  /**
   * Create a map of the table details. Each processor will produce <code>TableRow</code>s destined
   * to a different table. Each processor will provide the details about that table.
   *
   * @return map of table details keyed by table name
   */
  static Map<String, TableDetails> tableNameToTableDetailsMap(
      Map<String, MLApiResponseProcessor> processors) {
    Map<String, TableDetails> tableNameToTableDetailsMap = new HashMap<>();
    processors.forEach(
        (tableName, processor) ->
            tableNameToTableDetailsMap.put(tableName, processor.destinationTableDetails()));
    return tableNameToTableDetailsMap;
  }

  /**
   * Reads PubSub messages from the subscription provided by {@link
   * AnnotationPipelineOptions#getInputNotificationSubscription()}.
   *
   * <p>The messages are expected to confirm to the GCS notification message format defined in
   * https://cloud.google.com/storage/docs/pubsub-notifications
   *
   * <p>Notifications are filtered to have one of the supported content types: {@link
   * AnnotationPipeline#SUPPORTED_CONTENT_TYPES}.
   *
   * @return PCollection of GCS URIs
   */
  static PCollection<GCSFileInfo> convertPubSubNotificationsToGCSFileInfos(
      Pipeline p, AnnotationPipelineOptions options) {
    PCollection<GCSFileInfo> gcsFileInfos;
    PCollection<PubsubMessage> pubSubNotifications =
        p.begin()
            .apply(
                "Read PubSub",
                PubsubIO.readMessagesWithAttributes()
                    .fromSubscription(options.getInputNotificationSubscription()));
    gcsFileInfos =
        pubSubNotifications
            .apply(
                "PubSub to GCS URIs",
                ParDo.of(PubSubNotificationToGCSInfoDoFn.create(SUPPORTED_CONTENT_TYPES)))
            .apply(
                "Fixed Window",
                Window.<GCSFileInfo>into(
                        FixedWindows.of(Duration.standardSeconds(options.getWindowInterval())))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.standardMinutes(15)));
    return gcsFileInfos;
  }

  /**
   * Reads the GCS objects provided by {@link AnnotationPipelineOptions#getFileList()}.
   *
   * <p>The file list can contain multiple entries. Each entry can contain wildcards supported by
   * {@link FileIO#matchAll()}.
   *
   * <p>Files are filtered based on their suffixes as defined in {@link
   * AnnotationPipeline#ACCEPTED_FILE_PATTERN}.
   *
   * @return PCollection of GCS URIs
   */
  static PCollection<GCSFileInfo> listGCSFiles(Pipeline p, AnnotationPipelineOptions options) {
    PCollection<GCSFileInfo> fileInfos;
    PCollection<Metadata> allFiles =
        p.begin()
            .apply("Get File List", Create.of(options.getFileList()))
            .apply("Match GCS Files", FileIO.matchAll());
    fileInfos =
        allFiles
            .apply(
                ParDo.of(
                    new DoFn<Metadata, GCSFileInfo>() {
                      private static final long serialVersionUID = 1L;

                      @ProcessElement
                      public void processElement(
                          @Element Metadata metadata, OutputReceiver<GCSFileInfo> out) {
                        out.output(GCSUtils.getFileInfo(metadata.resourceId().toString()));
                      }
                    }))
            .apply(
                "Filter out non-image files",
                Filter.by(
                    (SerializableFunction<GCSFileInfo, Boolean>)
                        fileName -> {
                          totalFiles.inc();
                          if (fileName
                              .getUri()
                              .matches("(^(?i).*\\.(" + ACCEPTED_FILE_PATTERN + ")$)")) {
                            return true;
                          }
                          LOG.warn("File {} does not contain a valid extension", fileName);
                          rejectedFiles.inc();
                          return false;
                        }));
    return fileInfos;
  }

  /**
   * Creates a map of well-known {@link MLApiResponseProcessor}s.
   *
   * <p>If additional processors are needed they should be configured in this method.
   */
  private static Map<String, MLApiResponseProcessor> configureProcessors(
      AnnotationPipelineOptions options) {
    Map<String, MLApiResponseProcessor> result = new HashMap<>();

    // Image processors
    // ------------------------------------------------------------------------------

    String tableName = options.getImageLabelAnnotationTable();
    result.put(
        tableName,
        new LabelAnnotationProcessor(
            tableName,
            options.getMetadataKeys(),
            options.getRelevantImageLabels(),
            options.getImageLabelAnnotationScoreThreshold()));

    tableName = options.getImageLandmarkAnnotationTable();
    result.put(
        tableName,
        new LandmarkAnnotationProcessor(
            tableName,
            options.getMetadataKeys(),
            options.getRelevantImageLandmarks(),
            options.getImageLandmarkAnnotationScoreThreshold()));

    tableName = options.getImageLogoAnnotationTable();
    result.put(
        tableName,
        new LogoAnnotationProcessor(
            tableName,
            options.getMetadataKeys(),
            options.getRelevantLogos(),
            options.getLogoAnnotationScoreThreshold()));

    tableName = options.getImageFaceAnnotationTable();
    result.put(
        tableName,
        new FaceAnnotationProcessor(
            tableName,
            options.getMetadataKeys(),
            options.getFaceAnnotationDetectionConfidenceThreshold()));

    tableName = options.getImagePropertiesTable();
    result.put(tableName, new ImagePropertiesProcessor(tableName, options.getMetadataKeys()));

    tableName = options.getImageCropHintAnnotationTable();
    result.put(
        tableName,
        new CropHintAnnotationProcessor(
            tableName,
            options.getMetadataKeys(),
            options.getImageCropAnnotationConfidenceThreshold()));

    tableName = options.getErrorLogTable();
    result.put(tableName, new ErrorProcessor(tableName, options.getMetadataKeys()));

    // Video processors
    // ------------------------------------------------------------------------------

    tableName = options.getVideoObjectTrackingAnnotationTable();
    result.put(
        tableName,
        new VideoObjectTrackingAnnotationProcessor(
            tableName,
            options.getMetadataKeys(),
            options.getRelevantObjectTrackingEntities(),
            options.getObjectTrackingConfidenceThreshold()));

    tableName = options.getVideoLabelAnnotationTable();
    result.put(
        tableName,
        new VideoLabelAnnotationProcessor(
            tableName,
            options.getMetadataKeys(),
            options.getRelevantVideoLabelEntities(),
            options.getVideoLabelConfidenceThreshold()));

    return result;
  }
}
