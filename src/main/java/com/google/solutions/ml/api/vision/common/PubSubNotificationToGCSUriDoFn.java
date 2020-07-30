package com.google.solutions.ml.api.vision.common;

import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubNotificationToGCSUriDoFn extends DoFn<PubsubMessage, String> {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubNotificationToGCSUriDoFn.class);
  @ProcessElement
  public void processElement(ProcessContext c) {
    String eventType = c.element().getAttribute("eventType");
    if (! eventType.equalsIgnoreCase(Util.ALLOWED_NOTIFICATION_EVENT_TYPE)) {
      LOG.warn("Event Type Not Supported {}", eventType);
      return;
    }

    String bucket = c.element().getAttribute("bucketId");
    String object = c.element().getAttribute("objectId");
    GcsPath uri = GcsPath.fromComponents(bucket, object);
    String fileName = uri.toString();

    c.output(fileName);

    LOG.debug("GCS URI: {}", fileName);
  }
}
