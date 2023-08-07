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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Writes TableRows to a {@link DynamicDestinations}. */
public class BigQueryDynamicWriteTransform
    extends PTransform<PCollection<KV<BQDestination, TableRow>>, WriteResult> {

  private static final long serialVersionUID = 1L;

  private final DynamicDestinations<KV<BQDestination, TableRow>, BQDestination> destinations;

  public BigQueryDynamicWriteTransform(
      DynamicDestinations<KV<BQDestination, TableRow>, BQDestination> destinations) {
    this.destinations = destinations;
  }

  @Override
  public WriteResult expand(PCollection<KV<BQDestination, TableRow>> input) {
    return input.apply(
        "BQ Write",
        BigQueryIO.<KV<BQDestination, TableRow>>write()
            .to(destinations)
            .withFormatFunction(KV::getValue)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withoutValidation()
            .withClustering()
            .ignoreInsertIds()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }
}
