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

import com.google.common.collect.ImmutableList;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class BatchRequestsTransformTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBatching() {
    GCSFileInfo fileInfo1 =
        new GCSFileInfo("gs://bucket/example1.jpg", "image/jpeg", new HashMap<>());
    GCSFileInfo fileInfo2 =
        new GCSFileInfo("gs://bucket/example2.jpg", "image/jpeg", new HashMap<>());
    GCSFileInfo fileInfo3 =
        new GCSFileInfo("gs://bucket/example3.jpg", "image/jpeg", new HashMap<>());
    GCSFileInfo fileInfo4 =
        new GCSFileInfo("gs://bucket/example4.jpg", "image/jpeg", new HashMap<>());
    GCSFileInfo fileInfo5 =
        new GCSFileInfo("gs://bucket/example5.jpg", "image/jpeg", new HashMap<>());
    Create.Values<GCSFileInfo> fileInfos =
        Create.of(fileInfo1, fileInfo2, fileInfo3, fileInfo4, fileInfo5);
    PCollection<Iterable<GCSFileInfo>> batchedFileInfos =
        pipeline.apply(fileInfos).apply(BatchRequestsTransform.create(2, 50));
    List<GCSFileInfo> expectedItems =
        new ArrayList<>(ImmutableList.of(fileInfo1, fileInfo2, fileInfo3, fileInfo4, fileInfo5));
    List<Integer> expectedBatchSizes = new ArrayList<>(ImmutableList.of(2, 2, 1));
    PAssert.that(batchedFileInfos)
        .satisfies(
            batches -> {
              batches.forEach(
                  batch -> {
                    AtomicReference<Integer> batchSize = new AtomicReference<>(0);
                    batch.forEach(
                        fileInfo -> {
                          // Make sure the item is in the expected list
                          if (!expectedItems.remove(fileInfo)) throw new AssertionError();
                          batchSize.getAndSet(batchSize.get() + 1);
                        });
                    // Make sure the batch size is expected
                    if (!expectedBatchSizes.remove(batchSize.get())) throw new AssertionError();
                  });
              // Make sure no other item exists other than the expected ones
              assert expectedItems.size() == 0;
              return null;
            });

    pipeline.run().waitUntilFinish();
  }
}
