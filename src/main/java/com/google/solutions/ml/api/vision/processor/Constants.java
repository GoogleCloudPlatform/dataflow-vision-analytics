/*
 * Copyright 2020 Google LLC
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

package com.google.solutions.ml.api.vision.processor;

import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Helper interface for common image annotation response processor constants
 */
interface Constants {

  interface Field {
    String BOUNDING_POLY = "bounding_poly";
    String FD_BOUNDING_POLY = "fd_bounding_poly";
    String LOCATIONS = "locations";
    String VERTICES = "vertices";
    String LANDMARKS = "landmarks";
    String FACE_LANDMARK_POSITION = "position";
    String FACE_LANDMARK_TYPE = "type";
    String DETECTION_CONFIDENCE = "detection_confidence";
    String LANDMARKING_CONFIDENCE = "landmarking_confidence";
    String JOY_LIKELIHOOD = "joy_likelihood";
    String SORROW_LIKELIHOOD = "sorrow_likelihood";
    String ANGER_LIKELIHOOD = "anger_likelihood";
    String SURPISE_LIKELIHOOD = "surprise_likelihood";
    String GCS_URI_FIELD = "gcs_uri";
    String TIMESTAMP_FIELD = "transaction_timestamp";
    String MID_FIELD = "mid";
    String DESCRIPTION_FIELD = "description";
    String TOPICALITY_FIELD = "topicality";
    String SCORE_FIELD = "score";
    String STACK_TRACE = "stack_trace";

    String VERTEX_X = "x";
    String VERTEX_Y = "y";
    String VERTEX_Z = "z";

    String DOMINANT_COLORS = "dominant_colors";
    String PIXEL_FRACTION = "pixel_fraction";
    String COLOR_RED = "red";
    String COLOR_GREEN = "green";
    String COLOR_BLUE = "blue";
    String COLOR_ALPHA = "alpha";
    String COLOR = "color";
    String COLORS = "colors";
  }

  List<TableFieldSchema> VERTEX_FIELDS = Arrays.asList(
      new TableFieldSchema().setName(Field.VERTEX_X).setType("FLOAT").setMode("REQUIRED"),
      new TableFieldSchema().setName(Field.VERTEX_Y).setType("FLOAT").setMode("REQUIRED")
  );

  List<TableFieldSchema> POSITION_FIELDS = Arrays.asList(
      new TableFieldSchema().setName(Field.VERTEX_X).setType("FLOAT").setMode("REQUIRED"),
      new TableFieldSchema().setName(Field.VERTEX_Y).setType("FLOAT").setMode("REQUIRED"),
      new TableFieldSchema().setName(Field.VERTEX_Z).setType("FLOAT").setMode("NULLABLE")
  );

  List<TableFieldSchema> POLYGON_FIELDS = Collections.singletonList(
      new TableFieldSchema().setName(Field.VERTICES).setType("RECORD").setMode("REPEATED")
          .setFields(VERTEX_FIELDS)
  );
}
