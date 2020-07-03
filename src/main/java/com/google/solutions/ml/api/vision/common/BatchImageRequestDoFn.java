/*
 * Copyright 2020 Google LLC
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class BatchImageRequestDoFn extends DoFn<KV<Integer, String>, List<String>> {
  public static final Logger LOG = LoggerFactory.getLogger(BatchImageRequestDoFn.class);

  private Integer batchSize;

  public BatchImageRequestDoFn(Integer batchSize) {
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
