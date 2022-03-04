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
package com.google.solutions.annotation.pubsub;

import com.google.common.collect.ImmutableSet;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;

public class PubSubNotificationToGCSInfoDoFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final ObjectMapper jsonMapper = new ObjectMapper();

  private static Set<String> getSampleSupportedContentTypes() {
    return new HashSet<>(ImmutableSet.of("image/jpeg", "image/png"));
  }

  private static Map<String, Object> getSamplePayload() {
    return new HashMap<>(Map.of("contentType", "image/jpeg", "metadata", Map.of("foo", "bar")));
  }

  private static Map<String, String> getSampleAttributes() {
    return new HashMap<>(
        Map.of(
            "eventType", "OBJECT_FINALIZE",
            "bucketId", "mybucket",
            "objectId", "example.jpg"));
  }

  @Test
  public void testSuccess() throws IOException {
    PubsubMessage message =
        new PubsubMessage(jsonMapper.writeValueAsBytes(getSamplePayload()), getSampleAttributes());
    PCollection<GCSFileInfo> fileInfos =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(PubSubNotificationToGCSInfoDoFn.create(getSampleSupportedContentTypes())));
    // Make sure the event was accepted and properly processed
    PAssert.that(fileInfos)
        .containsInAnyOrder(
            new GCSFileInfo("gs://mybucket/example.jpg", "image/jpeg", Map.of("foo", "bar")));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testUnsupportedContentType() throws IOException {
    Map<String, Object> payload = getSamplePayload();
    PubsubMessage message =
        new PubsubMessage(jsonMapper.writeValueAsBytes(payload), getSampleAttributes());

    // Remove the input file's content type from the list of supported ones
    Set<String> supportedContentTypes = getSampleSupportedContentTypes();
    supportedContentTypes.remove("image/jpeg");

    PCollection<GCSFileInfo> fileInfos =
        pipeline
            .apply(Create.of(message))
            .apply(ParDo.of(PubSubNotificationToGCSInfoDoFn.create(supportedContentTypes)));
    // Make sure the event was rejected
    PAssert.that(fileInfos).empty();
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testUnsupportedEventType() throws IOException {
    Map<String, String> attributes = getSampleAttributes();
    attributes.put("eventType", "XXXXX");
    PubsubMessage message =
        new PubsubMessage(jsonMapper.writeValueAsBytes(getSamplePayload()), attributes);
    PCollection<GCSFileInfo> fileInfos =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(PubSubNotificationToGCSInfoDoFn.create(getSampleSupportedContentTypes())));
    // Make sure the event was rejected
    PAssert.that(fileInfos).empty();
    pipeline.run().waitUntilFinish();
  }
}
