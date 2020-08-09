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
import org.apache.beam.sdk.options.Validation;

/** Interface to store pipeline options provided by the user */
public interface VisionAnalyticsPipelineOptions extends DataflowPipelineOptions {

  @Description("Pub/Sub subscription to receive messages from")
  String getSubscriberId();

  void setSubscriberId(String value);

  @Description("Google Cloud Storage files to process")
  String getFileList();

  void setFileList(String value);

  @Description("Key range")
  @Default.Integer(1)
  Integer getKeyRange();

  void setKeyRange(Integer value);

  @Description("Image batch size")
  @Default.Integer(1)
  Integer getBatchSize();

  void setBatchSize(Integer value);

  @Description("Window interval in seconds (default is 5)")
  @Default.Integer(5)
  Integer getWindowInterval();

  void setWindowInterval(Integer value);

  @Description("BigQuery dataset")
  @Validation.Required
  String getDatasetName();

  void setDatasetName(String value);

  @Description("Project id to be used for Vision API requests and BigQuery dataset")
  @Validation.Required
  String getVisionApiProjectId();

  void setVisionApiProjectId(String value);

  @Description("Vision API features to use")
  @Validation.Required
  List<Feature.Type> getFeatures();

  void setFeatures(List<Feature.Type> value);

  @Description("Simulate annotations")
  @Default.Boolean(false)
  boolean isSimulate();

  void setSimulate(boolean value);

  @Description("Table name for label annotations")
  @Default.String("label_annotation")
  String getLabelAnnotationTable();

  void setLabelAnnotationTable(String value);

  @Description("Table name for landmark annotations")
  @Default.String("landmark_annotation")
  String getLandmarkAnnotationTable();

  void setLandmarkAnnotationTable(String value);

  @Description("Table name for logo annotations")
  @Default.String("logo_annotation")
  String getLogoAnnotationTable();

  void setLogoAnnotationTable(String value);

  @Description("Table name for face annotations")
  @Default.String("face_annotation")
  String getFaceAnnotationTable();

  void setFaceAnnotationTable(String value);

  @Description("Table name for error logs")
  @Default.String("error_log")
  String getErrorLogTable();

  void setErrorLogTable(String value);
}
