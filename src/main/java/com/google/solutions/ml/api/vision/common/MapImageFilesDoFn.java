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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class MapImageFilesDoFn extends DoFn<ReadableFile, KV<String, String>> {
  public static final Logger LOG = LoggerFactory.getLogger(MapImageFilesDoFn.class);

  public static TupleTag<KV<String, String>> successTag = new TupleTag<KV<String, String>>() {};
  public static TupleTag<KV<String, TableRow>> failureTag = new TupleTag<KV<String, TableRow>>() {};

  /** Allowed image extension supported by Vision API */
  private static final String IMAGE_PATTERN = "([^\\s]+(\\.(?i)(/bmp|jpg|jpeg|gif|png))$)";
  /** Error message if no valid extension found */
  private static final String NO_VALID_EXT_FOUND_ERROR_MESSAGE =
      "File {} does not contain a valid extension (.bmp, .jpg, .jpeg, .gif, or .png)";

  @ProcessElement
  public void processElement(@Element ReadableFile file, MultiOutputReceiver out) {
    String imageFileName = file.getMetadata().resourceId().getFilename().toString();
    String bucketName = file.getMetadata().resourceId().getCurrentDirectory().toString();
    try {
      if (imageFileName.matches(IMAGE_PATTERN)) {
        out.get(successTag).output(KV.of(bucketName, imageFileName));

      } else {
        LOG.error(NO_VALID_EXT_FOUND_ERROR_MESSAGE, imageFileName);

        out.get(failureTag)
            .output(
                KV.of(
                    Util.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
                    Util.toTableRow(
                        Row.withSchema(Util.errorSchema)
                            .addValues(
                                imageFileName,
                                Util.getTimeStamp(),
                                NO_VALID_EXT_FOUND_ERROR_MESSAGE,
                                null)
                            .build())));
      }
    } catch (Exception e) {

      LOG.error("Error {} Mapping Image File {}", e.getMessage(), imageFileName);
      out.get(failureTag)
          .output(
              KV.of(
                  Util.BQ_TABLE_NAME_MAP.get("BQ_ERROR_TABLE"),
                  Util.toTableRow(
                      Row.withSchema(Util.errorSchema)
                          .addValues(
                              imageFileName,
                              Util.getTimeStamp(),
                              e.getMessage(),
                              ExceptionUtils.getStackTrace(e))
                          .build())));
    }
  }
}
