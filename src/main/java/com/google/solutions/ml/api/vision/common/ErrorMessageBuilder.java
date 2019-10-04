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

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ErrorMessageBuilder class {@link ErrorMessageBuilder} creates a builder pattern for error message
 * thrown as part of the transform Message is wrapper in BigQuery Table Row
 */
@AutoValue
public abstract class ErrorMessageBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorMessageBuilder.class);

  @Nullable
  public abstract String fileName();

  public abstract String timeStamp();

  @Nullable
  public abstract TableRow tableRow();

  @Nullable
  public abstract String errorMessage();

  @Nullable
  public abstract String stackTrace();

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFileName(String imageName);

    public abstract Builder setTimeStamp(String timeStamp);

    public abstract Builder setErrorMessage(String errorMessage);

    public abstract Builder setStackTrace(String stackTrace);

    public abstract Builder setTableRow(TableRow row);

    public abstract ErrorMessageBuilder build();
  }

  public static Builder newBuilder() {
    return new AutoValue_ErrorMessageBuilder.Builder();
  }

  public ErrorMessageBuilder withTableRow(TableRow row) {

    List<TableCell> cells = new ArrayList<>();
    row.set("fileName", fileName());
    cells.add(new TableCell().set("file_name", fileName()));
    row.set("transaction_timestamp", timeStamp());
    cells.add(new TableCell().set("transaction_timestamp", timeStamp()));
    row.set("error_message", errorMessage());
    cells.add(new TableCell().set("error_message", errorMessage()));
    row.set("stack_trace", stackTrace());
    cells.add(new TableCell().set("stack_trace", stackTrace()));
    return toBuilder().setTableRow(row).build();
  }
}
