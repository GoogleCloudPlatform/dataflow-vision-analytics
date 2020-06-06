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
package com.google.solutions.ml.api.vision.common;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.Feature;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CreateFeatureList {@link CreateFeatureList} class creates a list of feature type from JSON config
 * and pass it on to Feature type proto as a side input
 */
public class CreateFeatureList extends DoFn<Iterable<String>, Feature> {
  public static final Logger LOG = LoggerFactory.getLogger(CreateFeatureList.class);
  /** Default Feature List is set to LABEL_DETECTION */
  private static final String DEFAULT_FEATURE_TYPE =
      "{\"featureConfig\":[{\"type\":\"LABEL_DETECTION\"}]}";

  public static TupleTag<Feature> successTag = new TupleTag<Feature>() {};
  public static TupleTag<KV<String, TableRow>> failureTag = new TupleTag<KV<String, TableRow>>() {};
  private String featureConfig;
  private Gson gson;
  public final String FEATURE_MAP_NOT_FOUND = "Feature property % not found";

  public CreateFeatureList(String featureConfig) {
    this.featureConfig = featureConfig != null ? featureConfig : DEFAULT_FEATURE_TYPE;
  }

  @Setup
  public void setup() {
    gson = new Gson();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws InvalidProtocolBufferException {

    JsonObject features = gson.fromJson(featureConfig, new TypeToken<JsonObject>() {}.getType());
    features
        .get("featureConfig")
        .getAsJsonArray()
        .forEach(
            element -> {
              try {
                c.output(VisionApiUtil.convertJsonToFeature(element.getAsJsonObject().toString()));
              } catch (InvalidProtocolBufferException e) {
                ErrorMessageBuilder errorBuilder =
                    ErrorMessageBuilder.newBuilder()
                        .setErrorMessage(e.getMessage())
                        .setStackTrace(e.getStackTrace().toString())
                        .setTimeStamp(VisionApiUtil.getTimeStamp())
                        .build()
                        .withTableRow(new TableRow());
                c.output(
                    failureTag,
                    KV.of(
                        VisionApiUtil.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
                        errorBuilder.tableRow()));
              }
            });
  }
}
