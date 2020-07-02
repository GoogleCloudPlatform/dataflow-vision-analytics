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

import com.google.cloud.vision.v1.Feature;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Interface to store pipeline options provided by the user */
public interface VisionAnalyticsPipelineOptions extends DataflowPipelineOptions {

  @Description("Subscriber Id to receive message from")
  String getSubscriberId();

  void setSubscriberId(String value);

  @Description("key range")
  @Default.Integer(1)
  Integer getKeyRange();

  void setKeyRange(Integer value);

  @Description("Image Batch Size")
  @Default.Integer(1)
  Integer getBatchSize();

  void setBatchSize(Integer value);

  @Description("Window Interval Default to 5 secs")
  @Default.Integer(5)
  Integer getWindowInterval();

  void setWindowInterval(Integer value);

  @Description("Big Query dataset")
  String getDatasetName();

  void setDatasetName(String value);

  @Description("Project id to be used Vision API request")
  String getVisionApiProjectId();

  void setVisionApiProjectId(String value);

  @Description("Features")
  List<Feature.Type> getFeatures();

  void setFeatures(List<Feature.Type> value);
}
