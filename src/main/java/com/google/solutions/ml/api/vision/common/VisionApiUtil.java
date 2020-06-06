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

import com.google.api.client.json.GenericJson;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.cloud.vision.v1.Feature;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.FieldMask;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util class to reuse some convert methods used in the pipeline multiple times */
public class VisionApiUtil {

  public static final Logger LOG = LoggerFactory.getLogger(VisionApiUtil.class);

  public static final String ALLOWED_NOTIFICATION_EVENT_TYPE = String.valueOf("OBJECT_FINALIZE");
  /** Allowed image extension supported by Vision API */
  public static final String FILE_PATTERN = "(^.*\\.(JPEG|jpeg|JPG|jpg|PNG|png|GIF|gif)$)";
  /** Error message if no valid extension found */
  public static final String NO_VALID_EXT_FOUND_ERROR_MESSAGE =
      "File {} does not contain a valid extension";
  /** Default window interval to create side inputs for header records. */
  public static final Duration WINDOW_INTERVAL = Duration.standardSeconds(5);
  /** Default interval for polling files in GCS. */
  public static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(5);
  /** Default batch size if value not provided in execution. */
  public static final Integer DEFAULT_BATCH_SIZE = 16;

  public static TupleTag<KV<String, TableRow>> failureTag = new TupleTag<KV<String, TableRow>>() {};

  public static TupleTag<KV<String, TableRow>> successTag = new TupleTag<KV<String, TableRow>>() {};

  public static Gson gson = new Gson();
  /** Process time stamp field added part of BigQuery Column */
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  public static final Map<String, String> BQ_TABLE_NAME_MAP =
      ImmutableMap.<String, String>builder()
          .put("BQ_TABLE_NAME_DEFAULT_MODE", "VISION_API_FINDINGS_RAW_JSON")
          .put("BQ_TABLE_NAME_LABEL_ANNOTATION", "VISION_API_FINDINGS_LABEL_DETECTION")
          .put("BQ_TABLE_NAME_FACE_ANNOTATION", "VISION_API_FINDINGS_FACE_DETECTION")
          .put("BQ_TABLE_NAME_LANDMARK_ANNOTATION", "VISION_API_FINDINGS_LANDMARK_DETECTION")
          .put("BQ_TABLE_NAME_LOGO_ANNOTATION", "VISION_API_FINDINGS_LOGO_DETECTION")
          .put("BQ_TABLE_NAME_CORP_HINTS_ANNOTATION", "VISION_API_FINDINGS_CORP_HINTS_DETECTION")
          .put("BQ_TABLE_NAME_FULL_TEXT_ANNOTATION", "VISION_API_FINDINGS_FULL_TEXT_DETECTION")
          .put("BQ_TABLE_NAME_IMAGE_PROP_ANNOTATION", "VISION_API_FINDINGS_IMAGE_PROP_DETECTION")
          .put(
              "BQ_TABLE_NAME_LOCALIZED_OBJECT_ANNOTATION",
              "VISION_API_FINDINGS_LOCALIZED_OBJECT_DETECTION")
          .put("BQ_TABLE_NAME_PRODUCT_SEARCH_RESULT", "VISION_API_FINDINGS_PRODUCT_SEARCH_RESULT")
          .put("BQ_TABLE_NAME_SAFE_SEARCH_ANNOTATION", "VISION_API_FINDINGS_SAFE_SEARCH_RESULT")
          .put("BQ_TABLE_NAME_TEXT_ANNOTATION", "VISION_API_FINDINGS_TEXT_DETECTION")
          .put("BQ_TABLE_NAME_WEB_DETECTION_ANNOTATION", "VISION_API_FINDINGS_WEB_DETECTION")
          .put("BQ_ERROR_TABLE", "VISION_API_ERROR_TABLE")
          .build();

  public static Feature convertJsonToFeature(String json) throws InvalidProtocolBufferException {

    Feature.Builder feature = Feature.newBuilder();
    TypeRegistry registry = TypeRegistry.newBuilder().add(feature.getDescriptorForType()).build();
    JsonFormat.Parser jFormatter = JsonFormat.parser().usingTypeRegistry(registry);
    if (jFormatter != null) {
      jFormatter.merge(json.toString(), feature);
    }

    return feature.build();
  }

  public static GenericJson convertProtoToJson(EntityAnnotation maskedAnnotation)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(maskedAnnotation), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson amendMetadata(GenericJson json, String imageName) {

    json.put("file_name", imageName);
    json.put("transaction_timestamp", getTimeStamp());
    LOG.debug("generic Json {}", json.toString());

    return json;
  }

  public static String getTimeStamp() {
    return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));
  }

  private static FieldMask convertStringToFieldMask(String selectedCloumns) {
    Iterable<String> paths = Arrays.asList(selectedCloumns.split(","));
    FieldMask.Builder fieldMaskBuilder = FieldMask.newBuilder();
    for (String path : paths) {
      if (path.isEmpty()) {
        continue;
      }
      fieldMaskBuilder.addPaths(path);
    }
    FieldMask masks = fieldMaskBuilder.build();
    LOG.debug("Field Mask Config {}", masks.toString());
    return fieldMaskBuilder.build();
  }

  public static Map<String, FieldMask> convertJsonToFieldMask(String json) throws Exception {

    Map<String, FieldMask> dataMap = new HashMap<String, FieldMask>();

    if (json != null) {
      JsonObject jsonConfig = gson.fromJson(json, new TypeToken<JsonObject>() {}.getType());
      jsonConfig
          .get("selectedColumns")
          .getAsJsonArray()
          .forEach(
              element -> {
                element
                    .getAsJsonObject()
                    .entrySet()
                    .forEach(
                        entry -> {
                          dataMap.put(
                              entry.getKey(),
                              convertStringToFieldMask(entry.getValue().getAsString()));
                        });
              });
    }
    return dataMap;
  }

  public static TableRow convertJsonToTableRow(GenericJson jsonFormat) throws IOException {
    TableRow row;
    String json = gson.toJson(jsonFormat);
    List<TableCell> cells = new ArrayList<>();
    try (InputStream inputStream =
        new ByteArrayInputStream(json.trim().getBytes(StandardCharsets.UTF_8))) {
      row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);

    } catch (IOException e) {
      LOG.error("Can't parse JSON message", e.toString());
      throw new RuntimeException("Failed to serialize json to table row: " + json, e);
    }

    jsonFormat.forEach(
        (key, value) -> {
          cells.add(new TableCell().set(key, value));
        });

    row.setF(cells);
    LOG.info("Row {}", row.toString());
    return row;
  }

  public static GenericJson convertImageAnnotationToJson(AnnotateImageResponse response)
      throws JsonSyntaxException, InvalidProtocolBufferException {
    return gson.fromJson(
        JsonFormat.printer().print(response), new TypeToken<GenericJson>() {}.getType());
  }

  public static GenericJson applyRawJsonFormat(String imageName, String featureType, Object value)
      throws InvalidProtocolBufferException {

    GenericJson json = amendMetadata(new GenericJson(), imageName);
    json.put("feature_type", featureType);
    json.put("raw_json_response", gson.toJson(value));
    LOG.debug("json output {}", gson.toJson(json));
    return json;
  }
}
