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

import com.google.auto.value.AutoValue;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.util.Collections;
import java.util.Random;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;

/**
 * Groups the requests into certain size batches. See {@link GroupIntoBatches} for effects of
 * windowing on the output of this transform.
 */
@AutoValue
public abstract class BatchRequestsTransform
    extends PTransform<PCollection<GCSFileInfo>, PCollection<Iterable<GCSFileInfo>>> {

  private static final long serialVersionUID = 1L;

  public abstract long getBatchSize();

  public abstract int getKeyRange();

  /**
   * @param batchSize should be between 1 and 16
   * @param keyRange determines the level of parallelism. Should be a positive non-zero integer.
   * @return a new transform
   */
  public static BatchRequestsTransform create(long batchSize, int keyRange) {
    return builder().setBatchSize(batchSize).setKeyRange(keyRange).build();
  }

  @Override
  public PCollection<Iterable<GCSFileInfo>> expand(PCollection<GCSFileInfo> input) {
    if (getBatchSize() > 1) {
      return input
          .apply(
              "Assign Keys",
              WithKeys.of(
                  new SerializableFunction<GCSFileInfo, Integer>() {
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public Integer apply(GCSFileInfo input) {
                      return random.nextInt(getKeyRange());
                    }
                  }))
          .apply("Group Into Batches", GroupIntoBatches.ofSize(getBatchSize()))
          .apply("Convert to Batches", Values.create());
    } else {
      return input.apply(
          "Convert to Iterable",
          ParDo.of(
              new DoFn<GCSFileInfo, Iterable<GCSFileInfo>>() {
                private static final long serialVersionUID = 1L;

                @ProcessElement
                public void process(
                    @Element GCSFileInfo element, OutputReceiver<Iterable<GCSFileInfo>> out) {
                  out.output(Collections.singleton(element));
                }
              }));
    }
  }

  public static Builder builder() {
    return new AutoValue_BatchRequestsTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBatchSize(long newBatchSize);

    public abstract Builder setKeyRange(int newKeyRange);

    public abstract BatchRequestsTransform build();
  }
}
