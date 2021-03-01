/*
 * Copyright 2021 Google LLC
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.solutions.annotation.AnnotationPipeline;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts PubSub notifications into GCS File URIs. */
@AutoValue
public abstract class PubSubNotificationToGCSInfoDoFn extends DoFn<PubsubMessage, GCSFileInfo> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PubSubNotificationToGCSInfoDoFn.class);

  public abstract Set<String> supportedContentTypes();

  public static PubSubNotificationToGCSInfoDoFn create(Set<String> supportedContentTypes) {
    return builder().supportedContentTypes(supportedContentTypes).build();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    PubsubMessage message = c.element();
    String eventType = message.getAttribute("eventType");
    if (!Objects.equals(eventType, "OBJECT_FINALIZE")) {
      LOG.warn("PubSub event type '{}' will not be processed", eventType);
      return;
    }
    AnnotationPipeline.totalFiles.inc();

    String bucket = message.getAttribute("bucketId");
    String object = message.getAttribute("objectId");
    GcsPath uri = GcsPath.fromComponents(bucket, object);
    String fileName = uri.toString();

    String contentType = getContentType(message);

    if (contentType != null
        && supportedContentTypes().stream().noneMatch(contentType::equalsIgnoreCase)) {
      AnnotationPipeline.rejectedFiles.inc();
      LOG.warn(
          "File {} is rejected - content type '{}' is not supported. "
              + "Refer to https://cloud.google.com/vision/docs/supported-files for details.",
          fileName,
          contentType);
      return;
    }

    c.output(new GCSFileInfo(fileName, contentType, getMetadata(message)));

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

  /**
   * Extract GCS object's metadata from PubSub payload
   *
   * @return metadata or null if none found.
   */
  private Map<String, String> getMetadata(PubsubMessage message) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode payloadJson = mapper.readTree(message.getPayload());
      JsonNode metadata = payloadJson.get("metadata");
      if (metadata != null) {
        return mapper.convertValue(metadata, new TypeReference<Map<String, String>>() {});
      }
    } catch (IOException e) {
      LOG.warn("Failed to parse pubsub payload: ", e);
    }
    return new HashMap<>();
  }

  public static Builder builder() {
    return new AutoValue_PubSubNotificationToGCSInfoDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder supportedContentTypes(Set<String> supportedContentTypes);

    public abstract PubSubNotificationToGCSInfoDoFn build();
  }
}
