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
package com.google.solutions.annotation.ml;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.solutions.annotation.AnnotationPipeline;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackOffUtils {

  public static final Logger LOG = LoggerFactory.getLogger(BackOffUtils.class);

  public static ExponentialBackOff createBackOff() {
    return new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(10 * 1000 /* 10 seconds */)
        .setMaxElapsedTimeMillis(10 * 60 * 1000 /* 10 minutes */)
        .setMaxIntervalMillis(90 * 1000 /* 90 seconds */)
        .setMultiplier(1.5)
        .setRandomizationFactor(0.5)
        .build();
  }

  /**
   * Attempts to backoff unless reaches the max elapsed time.
   *
   * @param backoff
   * @param e
   */
  public static void handleQuotaReachedException(
      ExponentialBackOff backoff, ResourceExhaustedException e) {
    AnnotationPipeline.numberOfQuotaExceededRequests.inc();
    long waitInMillis = 0;
    try {
      waitInMillis = backoff.nextBackOffMillis();
    } catch (IOException ioException) {
      // Will not occur with this implementation of Backoff.
    }
    if (waitInMillis == ExponentialBackOff.STOP) {
      LOG.warn("Reached the limit of backoff retries. Throwing the exception to the pipeline");
      throw e;
    }
    LOG.info("Received {}. Will retry in {} seconds.", e.getClass().getName(), waitInMillis / 1000);
    try {
      TimeUnit.MILLISECONDS.sleep(waitInMillis);
    } catch (InterruptedException interruptedException) {
      // Do nothing
    }
  }
}
