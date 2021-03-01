/*
 * Copyright 2021 Google LLC
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
package com.google.solutions.annotation.gcs;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class GCSFileInfo implements Serializable {

  public static final long serialVersionUID = 1L;

  private String uri;
  private String contentType;
  private Map<String, String> metadata;

  GCSFileInfo() {
    // Needed for AvroCoder
  }

  public GCSFileInfo(String uri, String contentType, Map<String, String> metadata) {
    this.uri = uri;
    this.contentType = contentType;
    this.metadata = metadata;
  }

  public String getUri() {
    return uri;
  }

  public String getContentType() {
    return contentType;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GCSFileInfo that = (GCSFileInfo) o;
    return Objects.equals(uri, that.uri)
        && Objects.equals(contentType, that.contentType)
        && metadata.equals(that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, contentType);
  }
}
