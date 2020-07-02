/*
 * Copyright (C) 2020 Google Inc.
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

import com.google.solutions.ml.api.vision.common.BigQueryDynamicTransform;
import com.google.solutions.ml.api.vision.common.ImageRequestDoFn;
import com.google.solutions.ml.api.vision.common.ProcessImageTransform;
import com.google.solutions.ml.api.vision.common.ReadImageTransform;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VisionAnalyticsPipeline {
  public static final Logger LOG = LoggerFactory.getLogger(VisionAnalyticsPipeline.class);

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * VisionAnalyticsPipeline#run(VisionAnalyticsPipelineOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
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

  public static PipelineResult run(VisionAnalyticsPipelineOptions options) throws Exception {

    Pipeline p = Pipeline.create(options);

    PCollection<List<String>> imageFiles =
        p.apply(
            "ReadTransform",
            ReadImageTransform.newBuilder()
                .setBatchSize(options.getBatchSize())
                .setWindowInterval(options.getWindowInterval())
                .setKeyRange(options.getKeyRange())
                .setSubscriber(options.getSubscriberId())
                .build());

    PCollectionTuple imageRequest =
        imageFiles.apply(
            "ImageRequest",
            ParDo.of(new ImageRequestDoFn(options.getFeatures()))
                .withOutputTags(
                    ImageRequestDoFn.successTag, TupleTagList.of(ImageRequestDoFn.failureTag)));

    imageRequest
        .get(ImageRequestDoFn.successTag)
        .apply("ProcessResponse", new ProcessImageTransform())
        .apply(
            "WriteToBq",
            BigQueryDynamicTransform.newBuilder()
                .setDatasetId(options.getDatasetName())
                .setProjectId(options.getVisionApiProjectId())
                .build());

    return p.run();
  }
}
