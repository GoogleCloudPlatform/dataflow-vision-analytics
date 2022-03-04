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
package com.google.solutions.annotation.bigquery;

import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;

/**
 * Interface used in combination with {@link
 * org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations} implementations. See {@link
 * BigQueryDynamicDestinations} for details.
 */
public interface TableSchemaProducer extends Serializable {
  TableSchema getTableSchema();
}
