/*
 * Copyright 2019 Google LLC
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
package com.google.solutions.ml.api.vision.common;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ReadImageTransform extends PTransform<PBegin, PCollection<List<String>>> {
  public static final Logger LOG = LoggerFactory.getLogger(ReadImageTransform.class);

  public abstract Integer windowInterval();

  public abstract Integer batchSize();

  public abstract Integer keyRange();

  public abstract String subscriber();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setWindowInterval(Integer pollInterval);

    public abstract Builder setKeyRange(Integer keyRange);

    public abstract Builder setSubscriber(String subscriberId);

    public abstract ReadImageTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_ReadImageTransform.Builder();
  }

  @Override
  public PCollection<List<String>> expand(PBegin input) {
    return input
        .apply(
            "ReadFileMetadata",
            PubsubIO.readMessagesWithAttributes().fromSubscription(subscriber()))
        .apply("ConvertToGCSUri", ParDo.of(new MapPubSubMessage()))
        .apply("AddRandomKey", WithKeys.of(new Random().nextInt(keyRange())))
        .apply("BatchImages", ParDo.of(new BatchRequestDoFn(batchSize())));
  }

  public class MapPubSubMessage extends DoFn<PubsubMessage, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String bucket = c.element().getAttribute("bucketId");
      String object = c.element().getAttribute("objectId");
      String eventType = c.element().getAttribute("eventType");
      GcsPath uri = GcsPath.fromComponents(bucket, object);
      if (eventType.equalsIgnoreCase(Util.ALLOWED_NOTIFICATION_EVENT_TYPE)) {
        String fileName = uri.toString();
        if (fileName.matches(Util.FILE_PATTERN)) {
          c.output(fileName);
          LOG.info("File Output {}", fileName);
        } else {
          LOG.warn(Util.NO_VALID_EXT_FOUND_ERROR_MESSAGE, fileName);
        }
      } else {
        LOG.warn("Event Type Not Supported {}", eventType);
      }
    }
  }
}
