package com.google.solutions.ml.api.vision.common;

import com.google.auto.value.AutoValue;
import java.util.Set;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class PubSubNotificationToGCSUriDoFn extends DoFn<PubsubMessage, String> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PubSubNotificationToGCSUriDoFn.class);

  abstract public Set<String> supportedContentTypes();

  public static PubSubNotificationToGCSUriDoFn create(Set<String> supportedContentTypes) {
    return builder()
        .supportedContentTypes(supportedContentTypes)
        .build();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    PubsubMessage message = c.element();
    String eventType = message.getAttribute("eventType");
    if (! eventType.equals("OBJECT_FINALIZE")) {
      LOG.warn("PubSub event type '{}' will not be processed", eventType);
      return;
    }
    String contentType = message.getAttribute("contentType");
    if(contentType != null && ! supportedContentTypes().contains(contentType)) {
      LOG.warn("Content type '{}' is not supported. "
          + "Refer to https://cloud.google.com/vision/docs/supported-files for details.", eventType);
      return;
    }

    String bucket = message.getAttribute("bucketId");
    String object = message.getAttribute("objectId");
    GcsPath uri = GcsPath.fromComponents(bucket, object);
    String fileName = uri.toString();

    c.output(fileName);

    LOG.debug("GCS URI: {}", fileName);
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
