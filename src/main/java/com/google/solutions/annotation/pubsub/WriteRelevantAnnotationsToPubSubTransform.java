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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class WriteRelevantAnnotationsToPubSubTransform
    extends PTransform<PCollection<KV<String, TableRow>>, PDone> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG =
      LoggerFactory.getLogger(WriteRelevantAnnotationsToPubSubTransform.class);

  public abstract String topicId();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTopicId(String topic);

    public abstract WriteRelevantAnnotationsToPubSubTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_WriteRelevantAnnotationsToPubSubTransform.Builder();
  }

  @Override
  public PDone expand(PCollection<KV<String, TableRow>> input) {

    return input
        .apply(
            "ConvertToJSON",
            ParDo.of(
                new DoFn<KV<String, TableRow>, String>() {
                  @ProcessElement
                  public void processContext(ProcessContext c) throws IOException {
                    String type = Objects.requireNonNull(c.element()).getKey();
                    TableRow row = Objects.requireNonNull(c.element()).getValue();
                    ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
                    TableRowJsonCoder.of().encode(row, jsonStream);
                    String json = jsonStream.toString(StandardCharsets.UTF_8.name());

                    // TODO: Figure out why the json string has a leading "?" character
                    // Remove leading "?" character
                    json = json.substring(1);

                    c.output(String.format("{\"type\": \"%s\", \"annotation\": %s}", type, json));
                  }
                }))
        .apply(
            "ConvertToPubSubMessage",
            ParDo.of(
                new DoFn<String, PubsubMessage>() {
                  @ProcessElement
                  public void processContext(ProcessContext c) {
                    LOG.info("Json {}", c.element());
                    c.output(
                        new PubsubMessage(Objects.requireNonNull(c.element()).getBytes(), null));
                  }
                }))
        .apply("PublishToPubSub", PubsubIO.writeMessages().to(topicId()));
  }
}
