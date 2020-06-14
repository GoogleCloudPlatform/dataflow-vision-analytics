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
import com.google.cloud.vision.v1.AnnotateImageResponse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessImageTransform class {@link ProcessImageTransform} abstracts the actual image processing
 * from main pipeline so that it can be reused
 */
public class ProcessImageTransform
    extends PTransform<
        PCollection<KV<String, AnnotateImageResponse>>, PCollection<KV<String, TableRow>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessImageTransform.class);

  @Override
  public PCollection<KV<String, TableRow>> expand(
      PCollection<KV<String, AnnotateImageResponse>> imageResponse) {

    PCollection<KV<String, TableRow>> outputRow =
        imageResponse.apply("FindImageTag", ParDo.of(new ImageResponseHandlerDoFn()));

    return outputRow;
  }
}
