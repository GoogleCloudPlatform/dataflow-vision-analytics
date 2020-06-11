/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.solutions.ml.api.vision.common;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapImageFiles extends DoFn<ReadableFile, KV<String, String>> {
  public static final Logger LOG = LoggerFactory.getLogger(MapImageFiles.class);

  public static TupleTag<KV<String, String>> successTag = new TupleTag<KV<String, String>>() {};
  public static TupleTag<KV<String, TableRow>> failureTag = new TupleTag<KV<String, TableRow>>() {};

  /** Allowed image extension supported by Vision API */
  private static final String IMAGE_PATTERN = "([^\\s]+(\\.(?i)(/bmp|jpg|jpeg|gif|png))$)";
  /** Error message if no valid extension found */
  private static final String NO_VALID_EXT_FOUND_ERROR_MESSAGE =
      "File {} does not contain a valid extension";

  @ProcessElement
  public void processElement(ProcessContext c) {
    ReadableFile file = c.element();
    String imageFileName = file.getMetadata().resourceId().getFilename().toString();
    String bucketName = file.getMetadata().resourceId().getCurrentDirectory().toString();
    try {
      if (imageFileName.matches(IMAGE_PATTERN)) {
        c.output(KV.of(bucketName, imageFileName));

      } else {
        LOG.error(NO_VALID_EXT_FOUND_ERROR_MESSAGE, imageFileName);
        ErrorMessageBuilder errorBuilder =
            ErrorMessageBuilder.newBuilder()
                .setErrorMessage(NO_VALID_EXT_FOUND_ERROR_MESSAGE)
                .setFileName(imageFileName)
                .setTimeStamp(Util.getTimeStamp())
                .build()
                .withTableRow(new TableRow());
        c.output(
            failureTag,
            KV.of(Util.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"), errorBuilder.tableRow()));
      }
    } catch (Exception e) {

      LOG.error("Error Mapping Image File", e.getMessage());
      ErrorMessageBuilder errorBuilder =
          ErrorMessageBuilder.newBuilder()
              .setErrorMessage(e.getMessage())
              .setFileName(imageFileName)
              .setTimeStamp(Util.getTimeStamp())
              .setStackTrace(e.getStackTrace().toString())
              .build()
              .withTableRow(new TableRow());
      c.output(
          failureTag, KV.of(Util.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"), errorBuilder.tableRow()));
    }
  }
}
