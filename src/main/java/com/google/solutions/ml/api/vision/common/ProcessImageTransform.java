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

import com.google.auto.value.AutoValue;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.protobuf.FieldMask;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessImageTransform class {@link ProcessImageTransform} abstracts the actual image processing
 * from main pipeline so that it can be reused
 */
@AutoValue
public abstract class ProcessImageTransform
    extends PTransform<PCollection<KV<String, AnnotateImageResponse>>, PCollectionTuple> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessImageTransform.class);

  @Nullable
  public abstract PCollectionView<Map<String, FieldMask>> selectedColumns();

  public abstract boolean jsonMode();

  public static Builder newBuilder() {
    return new AutoValue_ProcessImageTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSelectedColumns(
        PCollectionView<Map<String, FieldMask>> selectedColumns);

    public abstract Builder setJsonMode(boolean mode);

    public abstract ProcessImageTransform build();
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<String, AnnotateImageResponse>> imageResponse) {

    PCollectionTuple output =
        imageResponse.apply(
            "FindImageTag",
            ParDo.of(new ProcessImageResponse(jsonMode(), selectedColumns()))
                .withSideInputs(selectedColumns())
                .withOutputTags(
                    VisionApiUtil.successTag, TupleTagList.of(VisionApiUtil.failureTag)));

    return output;
  }
}
