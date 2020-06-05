/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.solutions.ml.api.vision;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.Feature;
import com.google.protobuf.FieldMask;
import com.google.solutions.ml.api.vision.common.BQDestination;
import com.google.solutions.ml.api.vision.common.CreateFeatureList;
import com.google.solutions.ml.api.vision.common.CreateImageReqest;
import com.google.solutions.ml.api.vision.common.MapImageFiles;
import com.google.solutions.ml.api.vision.common.ProcessImageTransform;
import com.google.solutions.ml.api.vision.common.VisionApiPipelineOptions;
import com.google.solutions.ml.api.vision.common.VisionApiUtil;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link VisionTextToBigQueryStreaming} is a streaming pipeline that reads image files from a
 * storage location (e.g. Google Cloud Storage), uses Cloud Vision API to classify into predefined
 * categories and store the results in BigQuery for analysis or feed into custom machine learning
 * model
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>GCS Bucket Containing Image Files
 *   <li>The BigQuery Dataset exists
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * Default Mode: Generate JSON output for Label Detection
 * gradle run -DmainClass=com.google.solutions.ml.api.vision.VisionTextToBigQueryStreaming
 * -Pargs=" --streaming --project=<project_id>
 * --runner=DataflowRunner
 * --inputFilePattern=gs://{bucket_path}/*.jpg
 * --datasetName=<dataset>
 * --visionApiProjectId=<project_id>
 * --enableStreamingEngine"
 * </pre>
 */
public class VisionTextToBigQueryStreaming {
  public static final Logger LOG = LoggerFactory.getLogger(VisionTextToBigQueryStreaming.class);

  /** Default window interval to create side inputs for header records. */
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(5);
  /** Default interval for polling files in GCS. */
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(5);
  /** Default batch size if value not provided in execution. */
  private static final Integer DEFAULT_BATCH_SIZE = 16;

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * VisionTextToBigQueryStreaming#run(VisionApiPipelineOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) throws Exception {

    VisionApiPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(VisionApiPipelineOptions.class);

    /** If selected columns parameter exist, setting up default mode to false */
    if (options.getSelectedColumns() != null) {
      options.setRawJsonMode(false);
    }
    run(options);
  }

  public static PipelineResult run(VisionApiPipelineOptions options) throws Exception {

    Pipeline p = Pipeline.create(options);

    /*
     * Side input to create a map of selected columns.
     */
    final PCollectionView<Map<String, FieldMask>> selectedColumnsMap =
        p.apply(
                Create.of(VisionApiUtil.convertJsonToFieldMask(options.getSelectedColumns()))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), ProtoCoder.of(FieldMask.class))))
            .apply(View.asMap());

    PCollection<KV<String, Iterable<String>>> imageFiles =
        p.apply(
                "Poll Input Files",
                FileIO.match()
                    .filepattern(options.getInputFilePattern())
                    .continuously(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))
            .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
            .apply(
                "Get File Path",
                ParDo.of(new MapImageFiles())
                    .withOutputTags(
                        MapImageFiles.successTag, TupleTagList.of(MapImageFiles.failureTag)))
            .get(MapImageFiles.successTag)
            .apply(
                "Fixed Window",
                Window.<KV<String, String>>into(FixedWindows.of(WINDOW_INTERVAL))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(
                                AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(Duration.ZERO)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(GroupIntoBatches.<String, String>ofSize(DEFAULT_BATCH_SIZE))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));

    /*
     * Side input to create the list of features
     */
    final PCollectionView<List<Feature>> featureList =
        imageFiles
            .apply(
                "Create Feature List",
                ParDo.of(new CreateFeatureList(options.getFeatureType()))
                    .withOutputTags(
                        CreateFeatureList.successTag,
                        TupleTagList.of(CreateFeatureList.failureTag)))
            .get(CreateFeatureList.successTag)
            .apply(View.asList());

    PCollectionTuple imageResponses =
        imageFiles
            .apply(
                "Create Image Request",
                ParDo.of(new CreateImageReqest(featureList))
                    .withSideInputs(featureList)
                    .withOutputTags(
                        CreateImageReqest.successTag,
                        TupleTagList.of(CreateImageReqest.failureTag)))
            .get(CreateImageReqest.successTag)
            .apply(
                "Process Image Response",
                ProcessImageTransform.newBuilder()
                    .setJsonMode(options.getRawJsonMode())
                    .setSelectedColumns(selectedColumnsMap)
                    .build());

    imageResponses
        .get(VisionApiUtil.successTag)
        .apply(
            "BQ Write",
            BigQueryIO.<KV<String, TableRow>>write()
                .to(new BQDestination(options.getDatasetName(), options.getVisionApiProjectId()))
                .withFormatFunction(
                    element -> {
                      return element.getValue();
                    })
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    return p.run();
  }
}
