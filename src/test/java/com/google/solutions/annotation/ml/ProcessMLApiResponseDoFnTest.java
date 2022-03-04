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
package com.google.solutions.annotation.ml;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.videointelligence.v1p3beta1.Entity;
import com.google.cloud.videointelligence.v1p3beta1.LabelAnnotation;
import com.google.cloud.videointelligence.v1p3beta1.StreamingAnnotateVideoResponse;
import com.google.cloud.videointelligence.v1p3beta1.StreamingVideoAnnotationResults;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.junit.Rule;
import org.junit.Test;

public class ProcessMLApiResponseDoFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final TupleTag<KV<BigQueryDestination, TableRow>> allRows =
      new TupleTag<KV<BigQueryDestination, TableRow>>() {};
  private static final TupleTag<KV<String, TableRow>> relevantRows =
      new TupleTag<KV<String, TableRow>>() {};

  private StreamingAnnotateVideoResponse createResponse(String entity) {
    return StreamingAnnotateVideoResponse.newBuilder()
        .setAnnotationResults(
            StreamingVideoAnnotationResults.newBuilder()
                .addLabelAnnotations(
                    LabelAnnotation.newBuilder()
                        .setEntity(
                            Entity.newBuilder()
                                .setDescriptionBytes(ByteString.copyFrom(entity.getBytes()))
                                .build())
                        .build())
                .build())
        .build();
  }

  @Test
  public void testSuccess() {
    Map<String, MLApiResponseProcessor> processors =
        Map.of("fake_table", new FakeProcessor("fake_table", Set.of()));

    GCSFileInfo fileInfo1 =
        new GCSFileInfo("gs://mybucket/example1.jpg", "image/jpeg", new HashMap<>());
    StreamingAnnotateVideoResponse response1 = createResponse("chocolate");

    GCSFileInfo fileInfo2 =
        new GCSFileInfo("gs://mybucket/example2.jpg", "image/jpeg", new HashMap<>());
    StreamingAnnotateVideoResponse response2 = createResponse("coffee");

    PCollection<KV<GCSFileInfo, GeneratedMessageV3>> annotatedFiles =
        pipeline.apply(Create.of(KV.of(fileInfo1, response1), KV.of(fileInfo2, response2)));
    PCollectionTuple annotationOutcome =
        annotatedFiles.apply(
            ParDo.of(
                    ProcessMLApiResponseDoFn.create(
                        ImmutableSet.copyOf(processors.values()), allRows, relevantRows))
                .withOutputTags(allRows, TupleTagList.of(relevantRows)));

    TableRow row1 = new TableRow();
    row1.put(Constants.Field.GCS_URI_FIELD, "gs://mybucket/example1.jpg");
    row1.set(Constants.Field.ENTITY, "chocolate");
    TableRow row2 = new TableRow();
    row2.put(Constants.Field.GCS_URI_FIELD, "gs://mybucket/example2.jpg");
    row2.set(Constants.Field.ENTITY, "coffee");

    PAssert.that(annotationOutcome.get(allRows))
        .containsInAnyOrder(
            KV.of(new BigQueryDestination("fake_table"), row1),
            KV.of(new BigQueryDestination("fake_table"), row2));
    PAssert.that(annotationOutcome.get(relevantRows)).containsInAnyOrder(KV.of("fake_type", row1));

    pipeline.run().waitUntilFinish();
  }
}
