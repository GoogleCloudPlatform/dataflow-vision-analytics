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

import static com.google.protobuf.ByteString.*;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class GCSUtils {

  private static final String APPLICATION_NAME = "my-app-name"; // FIXME
  private static Storage storageService;

  private static URI getURI(String gcsURI) {
    try {
      return new URI(gcsURI);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static GCSFileInfo getFileInfo(String gcsURI) {
    URI uri = getURI(gcsURI);
    Storage storageClient = getStorageClient();
    Storage.Objects.Get getObject;
    StorageObject object;
    try {
      getObject = storageClient.objects().get(uri.getHost(), uri.getPath().substring(1));
      object = getObject.execute();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new GCSFileInfo(gcsURI, object.getContentType(), object.getMetadata());
  }

  public static ByteString getBytes(String gcsURI) {
    URI uri = getURI(gcsURI);
    Storage storageClient = getStorageClient();
    Storage.Objects.Get getObject;
    try {
      getObject = storageClient.objects().get(uri.getHost(), uri.getPath().substring(1));
      getObject.getMediaHttpDownloader().setDirectDownloadEnabled(true);
      Output output = newOutput();
      getObject.executeMediaAndDownloadTo(output);
      return output.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Storage getStorageClient() {
    if (null == storageService) {
      HttpTransport httpTransport;
      GoogleCredentials credentials;
      try {
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        credentials =
            GoogleCredentials.getApplicationDefault().createScoped(StorageScopes.CLOUD_PLATFORM);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
      JacksonFactory jacksonFactory = JacksonFactory.getDefaultInstance();
      storageService =
          new Storage.Builder(httpTransport, jacksonFactory, requestInitializer)
              .setApplicationName(APPLICATION_NAME)
              .build();
    }
    return storageService;
  }
}
