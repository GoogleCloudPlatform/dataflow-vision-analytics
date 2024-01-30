/*
 * Copyright 2023 Google LLC
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts PubSub notifications into GCS File URIs. */
@AutoValue
public abstract class PubSubNotificationToGCSUriDoFn extends DoFn<PubsubMessage, String> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PubSubNotificationToGCSUriDoFn.class);

  public abstract Set<String> supportedContentTypes();

  public static PubSubNotificationToGCSUriDoFn create(Set<String> supportedContentTypes) {
    return builder().supportedContentTypes(supportedContentTypes).build();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    PubsubMessage message = c.element();
    String eventType = message.getAttribute("eventType");
    if (!Objects.equals(eventType, "OBJECT_FINALIZE")) {
      LOG.debug("PubSub event type '{}' will not be processed", eventType);
      return;
    }
    VisionAnalyticsPipeline.totalFiles.inc();

    String bucket = message.getAttribute("bucketId");
    String object = message.getAttribute("objectId");
    GcsPath uri = GcsPath.fromComponents(bucket, object);
    String fileName = uri.toString();

    String contentType = getContentType(message);

    if (contentType != null && !supportedContentTypes().contains(contentType)) {
      VisionAnalyticsPipeline.rejectedFiles.inc();
      LOG.warn(
          "File {} is rejected - content type '{}' is not supported. "
              + "Refer to https://cloud.google.com/vision/docs/supported-files for details.",
          fileName,
          contentType);
      return;
    }

    c.output(fileName);

    LOG.debug("GCS URI: {}", fileName);
  }

  /**
   * Extract content type from PubSub payload
   *
   * @return content type or null if none found.
   */
  private String getContentType(PubsubMessage message) {
    try {
      JsonNode payloadJson = new ObjectMapper().readTree(message.getPayload());
      JsonNode contentTypeNode = payloadJson.get("contentType");
      return contentTypeNode == null ? null : contentTypeNode.asText();
    } catch (IOException e) {
      LOG.warn("Failed to parse pubsub payload: ", e);
      return null;
    }
  }

  public static Builder builder() {
    return new AutoValue_PubSubNotificationToGCSUriDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder supportedContentTypes(Set<String> supportedContentTypes);

    public abstract PubSubNotificationToGCSUriDoFn build();
  }
}
