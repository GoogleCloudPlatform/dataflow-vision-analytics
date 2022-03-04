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
package com.google.solutions.annotation.ml.videointelligence;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.gax.rpc.BidiStream;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.cloud.videointelligence.v1p3beta1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.AnnotationPipeline;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import com.google.solutions.annotation.gcs.GCSUtils;
import com.google.solutions.annotation.ml.BackOffUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VideoAnnotator {

  public static final Logger LOG = LoggerFactory.getLogger(VideoAnnotator.class);
  private final List<StreamingFeature> features;
  private final StreamingVideoIntelligenceServiceClient client;
  BidiStream<StreamingAnnotateVideoRequest, StreamingAnnotateVideoResponse> streamCall;

  public VideoAnnotator(List<StreamingFeature> features) throws IOException {
    this.features = features;
    this.client = StreamingVideoIntelligenceServiceClient.create();
  }

  public void teardown() {
    if (client != null) {
      client.shutdown();
      try {
        int waitTime = 10;
        if (!client.awaitTermination(waitTime, TimeUnit.SECONDS)) {
          LOG.warn(
              "Failed to shutdown the annotation client after {} seconds. Closing client anyway.",
              waitTime);
        }
      } catch (InterruptedException e) {
        // Do nothing
      }
      client.close();
    }
  }

  private static StreamingVideoConfig getConfig(StreamingFeature feature) {
    StreamingObjectTrackingConfig objectTrackingConfig =
        StreamingObjectTrackingConfig.newBuilder().build();
    StreamingLabelDetectionConfig labelConfig = StreamingLabelDetectionConfig.newBuilder().build();
    StreamingVideoConfig.Builder builder =
        StreamingVideoConfig.newBuilder()
            .setObjectTrackingConfig(objectTrackingConfig)
            .setLabelDetectionConfig(labelConfig)
            .setFeature(feature);
    return builder.build();
  }

  public List<KV<GCSFileInfo, GeneratedMessageV3>> processFiles(Iterable<GCSFileInfo> fileInfos) {
    List<KV<GCSFileInfo, GeneratedMessageV3>> result = new ArrayList<>();

    // Download files' contents from GCS
    List<ByteString> gcsBytes = new ArrayList<>();
    fileInfos.forEach(
        fileInfo -> {
          ByteString bytes = GCSUtils.getBytes(fileInfo.getUri());
          gcsBytes.add(bytes);
        });

    ExponentialBackOff backoff = BackOffUtils.createBackOff();
    AtomicInteger counter = new AtomicInteger();
    fileInfos.forEach(
        fileInfo -> {
          ByteString bytes = gcsBytes.get(counter.get());
          // The Streaming API only accepts one feature at a time, so we send multiple requests.
          for (StreamingFeature feature : features) {
            while (true) {
              try {
                // Send the bytes to the Streaming Video API
                streamCall = client.streamingAnnotateVideoCallable().call();
                StreamingVideoConfig config = getConfig(feature);
                streamCall.send(
                    StreamingAnnotateVideoRequest.newBuilder().setVideoConfig(config).build());
                streamCall.send(
                    StreamingAnnotateVideoRequest.newBuilder().setInputContent(bytes).build());
                AnnotationPipeline.numberOfVideoApiRequests.inc();
                streamCall.closeSend();
                for (StreamingAnnotateVideoResponse response : streamCall) {
                  result.add(KV.of(fileInfo, response));
                }
                break;
              } catch (ResourceExhaustedException e) {
                BackOffUtils.handleQuotaReachedException(backoff, e);
              }
            }
          }
          counter.getAndIncrement();
        });
    return result;
  }
}
