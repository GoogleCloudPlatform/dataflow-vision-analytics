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
package com.google.solutions.ml.api.vision.processor;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.TableDetails;
import java.io.Serializable;
import org.apache.beam.sdk.values.KV;

/**
 * Implementors of this interface will process zero to many TableRows to persist to a specific
 * BigTable table.
 */
public interface AnnotateImageResponseProcessor extends Serializable {

  /**
   * @param gcsURI annotation source
   * @param response from Google Cloud Vision API
   * @return key/value pair of a BigQuery destination and a TableRow to persist.
   */
  Iterable<KV<BQDestination, TableRow>> process(String gcsURI, AnnotateImageResponse response);

  /**
   * @return details of the table to persist to.
   */
  TableDetails destinationTableDetails();
}
