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

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.GeneratedMessageV3;
import com.google.solutions.annotation.bigquery.BigQueryDestination;
import com.google.solutions.annotation.bigquery.TableDetails;
import com.google.solutions.annotation.gcs.GCSFileInfo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementors of this interface will process zero to many TableRows to persist to a specific
 * BigTable table.
 */
public interface MLApiResponseProcessor extends Serializable {

  class ProcessorResult {
    public static final String IMAGE_ERROR = "image-error";
    public static final String IMAGE_PROPERTIES = "image-properties";
    public static final String IMAGE_LABEL = "image-label";
    public static final String IMAGE_LOGO = "image-logo";
    public static final String IMAGE_LANDMARK = "image-landmark";
    public static final String IMAGE_FACE = "image-face";
    public static final String IMAGE_CROP = "image-crop";
    public static final String VIDEO_OBJECT_TRACKING = "video-object-tracking";
    public static final String VIDEO_LABEL = "video-label";

    public String type;
    public BigQueryDestination destination;
    public List<TableRow> allRows;
    public List<TableRow> relevantRows;

    public ProcessorResult(String type, BigQueryDestination destination) {
      this.type = type;
      this.destination = destination;
      this.allRows = new ArrayList<>();
      this.relevantRows = new ArrayList<>();
    }
  }

  /**
   * @param fileInfo annotation source
   * @param response from Google Cloud ML Video Intelligence or Cloud ML Vision API
   * @return key/value pair of a BigQuery destination and a TableRow to persist.
   */
  ProcessorResult process(GCSFileInfo fileInfo, GeneratedMessageV3 response);

  /** @return details of the table to persist to. */
  TableDetails destinationTableDetails();

  /** @return true if the processor is meant to processor this type of response object. */
  boolean shouldProcess(GeneratedMessageV3 response);
}
