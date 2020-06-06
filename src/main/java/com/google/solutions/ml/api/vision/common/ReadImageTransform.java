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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
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
        .apply(
            "Fixed Window",
            Window.<KV<Integer, String>>into(
                    FixedWindows.of(Duration.standardSeconds(windowInterval())))
                .triggering(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO))
        .apply("BatchRequest", ParDo.of(new BatchRequest(batchSize())));
  }

  public static class BatchRequest extends DoFn<KV<Integer, String>, List<String>> {
    private Integer batchSize;

    public BatchRequest(Integer batchSize) {
      this.batchSize = batchSize;
    }

    @StateId("elementsBag")
    private final StateSpec<BagState<String>> elementsBag = StateSpecs.bag();

    @TimerId("eventTimer")
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<Integer, String> element,
        @StateId("elementsBag") BagState<String> elementsBag,
        @TimerId("eventTimer") Timer eventTimer,
        BoundedWindow w) {
      elementsBag.add(element.getValue());
      eventTimer.set(w.maxTimestamp());
    }

    @OnTimer("eventTimer")
    public void onTimer(
        @StateId("elementsBag") BagState<String> elementsBag, OutputReceiver<List<String>> output) {
      AtomicInteger bufferCount = new AtomicInteger();
      List<String> rows = new ArrayList<>();
      elementsBag
          .read()
          .forEach(
              element -> {
                boolean clearBuffer = (bufferCount.intValue() == batchSize.intValue());
                if (clearBuffer) {
                  LOG.info("Clear Buffer {}", rows.size());
                  output.output(rows);
                  rows.clear();
                  bufferCount.set(0);
                  rows.add(element);
                  bufferCount.getAndAdd(1);

                } else {
                  rows.add(element);
                  bufferCount.getAndAdd(1);
                }
              });
      if (!rows.isEmpty()) {
        LOG.info("Remaining rows {}", rows.size());
        output.output(rows);
      }
    }
  }

  public class MapPubSubMessage extends DoFn<PubsubMessage, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String bucket = c.element().getAttribute("bucketId");
      String object = c.element().getAttribute("objectId");
      String eventType = c.element().getAttribute("eventType");
      GcsPath uri = GcsPath.fromComponents(bucket, object);
      if (eventType.equalsIgnoreCase(VisionApiUtil.ALLOWED_NOTIFICATION_EVENT_TYPE)) {
        String fileName = uri.toString();
        if (fileName.matches(VisionApiUtil.FILE_PATTERN)) {
          c.output(fileName);
          LOG.info("File Output {}", fileName);
        } else {
          LOG.warn(VisionApiUtil.NO_VALID_EXT_FOUND_ERROR_MESSAGE, fileName);
        }
      } else {
        LOG.warn("Event Type Not Supported {}", eventType);
      }
    }
  }
}
