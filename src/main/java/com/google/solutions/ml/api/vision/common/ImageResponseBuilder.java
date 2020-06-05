/*
 * Copyright (C) 2020 Google Inc.
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
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.vision.v1.CropHintsAnnotation;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.cloud.vision.v1.FaceAnnotation;
import com.google.cloud.vision.v1.ImageProperties;
import com.google.cloud.vision.v1.LocalizedObjectAnnotation;
import com.google.cloud.vision.v1.ProductSearchResults;
import com.google.cloud.vision.v1.SafeSearchAnnotation;
import com.google.cloud.vision.v1.TextAnnotation;
import com.google.cloud.vision.v1.WebDetection;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.FieldMaskUtil;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImageResponseBuilder class {@link ImageResponseBuilder} creates a builder pattern for each
 * annotation type by parsing the image response. This class sets up table row based on the
 * annotation response
 */
@AutoValue
public abstract class ImageResponseBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ImageResponseBuilder.class);

  public abstract String imageName();

  public abstract TableRow tableRow();

  public abstract FieldMask fieldMask();

  @Nullable
  public abstract EntityAnnotation entityAnnotation();

  @Nullable
  public abstract FaceAnnotation faceAnnotation();

  @Nullable
  public abstract CropHintsAnnotation cropHintsAnnotation();

  @Nullable
  public abstract TextAnnotation textAnnotation();

  @Nullable
  public abstract ImageProperties imagePropAnnotation();

  @Nullable
  public abstract LocalizedObjectAnnotation localizedObjAnnotation();

  @Nullable
  public abstract ProductSearchResults productSchResultAnnotation();

  @Nullable
  public abstract SafeSearchAnnotation safeSchResultAnnotation();

  @Nullable
  public abstract WebDetection webDetectionAnnotation();

  abstract Builder toBuilder();

  public static Builder newBuilder() {
    return new AutoValue_ImageResponseBuilder.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setImageName(String imageName);

    public abstract Builder setTableRow(TableRow tableRow);

    public abstract Builder setFieldMask(FieldMask fieldMask);

    public abstract Builder setEntityAnnotation(EntityAnnotation entityAnnotaton);

    public abstract Builder setFaceAnnotation(FaceAnnotation faceAnnotaton);

    public abstract Builder setCropHintsAnnotation(CropHintsAnnotation cropHintsAnnotation);

    public abstract Builder setTextAnnotation(TextAnnotation textAnnotation);

    public abstract Builder setImagePropAnnotation(ImageProperties imagePropAnnotation);

    public abstract Builder setLocalizedObjAnnotation(
        LocalizedObjectAnnotation localizedObjAnnotation);

    public abstract Builder setSafeSchResultAnnotation(
        SafeSearchAnnotation safeSchResultAnnotation);

    public abstract Builder setProductSchResultAnnotation(
        ProductSearchResults productSchResultAnnotation);

    public abstract Builder setWebDetectionAnnotation(WebDetection webDetectionAnnotation);

    public abstract ImageResponseBuilder build();
  }

  public ImageResponseBuilder withEntityAnnotation(EntityAnnotation annotation)
      throws JsonSyntaxException, IOException {
    EntityAnnotation.Builder maskedAnnotation = EntityAnnotation.newBuilder();
    FieldMaskUtil.merge(fieldMask(), annotation, maskedAnnotation);
    GenericJson json =
        VisionApiUtil.gson.fromJson(
            JsonFormat.printer().print(maskedAnnotation),
            new TypeToken<GenericJson>() {}.getType());
    json = VisionApiUtil.amendMetadata(json, imageName());
    TableRow tableRow = VisionApiUtil.convertJsonToTableRow(json);
    return toBuilder().setEntityAnnotation(maskedAnnotation.build()).setTableRow(tableRow).build();
  }

  public ImageResponseBuilder withFaceAnnotation(FaceAnnotation annotation)
      throws JsonSyntaxException, IOException {
    FaceAnnotation.Builder maskedAnnotation = FaceAnnotation.newBuilder();
    FieldMaskUtil.merge(fieldMask(), annotation, maskedAnnotation);
    GenericJson json =
        VisionApiUtil.gson.fromJson(
            JsonFormat.printer().print(maskedAnnotation),
            new TypeToken<GenericJson>() {}.getType());
    json = VisionApiUtil.amendMetadata(json, imageName());
    TableRow tableRow = VisionApiUtil.convertJsonToTableRow(json);
    return toBuilder().setFaceAnnotation(maskedAnnotation.build()).setTableRow(tableRow).build();
  }

  public ImageResponseBuilder withCropHintsAnnotation(CropHintsAnnotation annotation)
      throws JsonSyntaxException, IOException {
    LOG.info("original proto {}", annotation);

    CropHintsAnnotation.Builder maskedAnnotation = CropHintsAnnotation.newBuilder();
    FieldMaskUtil.merge(fieldMask(), annotation, maskedAnnotation);
    GenericJson json =
        VisionApiUtil.gson.fromJson(
            JsonFormat.printer().print(maskedAnnotation),
            new TypeToken<GenericJson>() {}.getType());
    json = VisionApiUtil.amendMetadata(json, imageName());
    TableRow tableRow = VisionApiUtil.convertJsonToTableRow(json);
    return toBuilder()
        .setCropHintsAnnotation(maskedAnnotation.build())
        .setTableRow(tableRow)
        .build();
  }

  public ImageResponseBuilder withTextAnnotation(TextAnnotation annotation)
      throws JsonSyntaxException, IOException {
    TextAnnotation.Builder maskedAnnotation = TextAnnotation.newBuilder();
    FieldMaskUtil.merge(fieldMask(), annotation, maskedAnnotation);
    GenericJson json =
        VisionApiUtil.gson.fromJson(
            JsonFormat.printer().print(maskedAnnotation),
            new TypeToken<GenericJson>() {}.getType());
    json = VisionApiUtil.amendMetadata(json, imageName());
    TableRow tableRow = VisionApiUtil.convertJsonToTableRow(json);
    return toBuilder().setTextAnnotation(maskedAnnotation.build()).setTableRow(tableRow).build();
  }

  public ImageResponseBuilder withImagePropAnnotation(ImageProperties annotation)
      throws JsonSyntaxException, IOException {
    ImageProperties.Builder maskedAnnotation = ImageProperties.newBuilder();
    FieldMaskUtil.merge(fieldMask(), annotation, maskedAnnotation);
    GenericJson json =
        VisionApiUtil.gson.fromJson(
            JsonFormat.printer().print(maskedAnnotation),
            new TypeToken<GenericJson>() {}.getType());
    json = VisionApiUtil.amendMetadata(json, imageName());
    TableRow tableRow = VisionApiUtil.convertJsonToTableRow(json);
    return toBuilder()
        .setImagePropAnnotation(maskedAnnotation.build())
        .setTableRow(tableRow)
        .build();
  }

  public ImageResponseBuilder withLocalizedObjAnnotation(LocalizedObjectAnnotation annotation)
      throws JsonSyntaxException, IOException {
    LocalizedObjectAnnotation.Builder maskedAnnotation = LocalizedObjectAnnotation.newBuilder();
    FieldMaskUtil.merge(fieldMask(), annotation, maskedAnnotation);
    GenericJson json =
        VisionApiUtil.gson.fromJson(
            JsonFormat.printer().print(maskedAnnotation),
            new TypeToken<GenericJson>() {}.getType());
    json = VisionApiUtil.amendMetadata(json, imageName());
    TableRow tableRow = VisionApiUtil.convertJsonToTableRow(json);
    return toBuilder()
        .setLocalizedObjAnnotation(maskedAnnotation.build())
        .setTableRow(tableRow)
        .build();
  }

  public ImageResponseBuilder withProductSchResultAnnotation(ProductSearchResults annotation)
      throws JsonSyntaxException, IOException {
    ProductSearchResults.Builder maskedAnnotation = ProductSearchResults.newBuilder();
    FieldMaskUtil.merge(fieldMask(), annotation, maskedAnnotation);
    GenericJson json =
        VisionApiUtil.gson.fromJson(
            JsonFormat.printer().print(maskedAnnotation),
            new TypeToken<GenericJson>() {}.getType());
    json = VisionApiUtil.amendMetadata(json, imageName());
    TableRow tableRow = VisionApiUtil.convertJsonToTableRow(json);
    return toBuilder()
        .setProductSchResultAnnotation(maskedAnnotation.build())
        .setTableRow(tableRow)
        .build();
  }

  public ImageResponseBuilder withSafeSchResultAnnotation(SafeSearchAnnotation annotation)
      throws JsonSyntaxException, IOException {
    SafeSearchAnnotation.Builder maskedAnnotation = SafeSearchAnnotation.newBuilder();
    FieldMaskUtil.merge(fieldMask(), annotation, maskedAnnotation);
    GenericJson json =
        VisionApiUtil.gson.fromJson(
            JsonFormat.printer().print(maskedAnnotation),
            new TypeToken<GenericJson>() {}.getType());
    json = VisionApiUtil.amendMetadata(json, imageName());
    TableRow tableRow = VisionApiUtil.convertJsonToTableRow(json);
    return toBuilder()
        .setSafeSchResultAnnotation(maskedAnnotation.build())
        .setTableRow(tableRow)
        .build();
  }

  public ImageResponseBuilder withWebDetectionAnnotation(WebDetection annotation)
      throws JsonSyntaxException, IOException {
    WebDetection.Builder maskedAnnotation = WebDetection.newBuilder();
    FieldMaskUtil.merge(fieldMask(), annotation, maskedAnnotation);
    GenericJson json =
        VisionApiUtil.gson.fromJson(
            JsonFormat.printer().print(maskedAnnotation),
            new TypeToken<GenericJson>() {}.getType());
    json = VisionApiUtil.amendMetadata(json, imageName());
    TableRow tableRow = VisionApiUtil.convertJsonToTableRow(json);
    return toBuilder()
        .setWebDetectionAnnotation(maskedAnnotation.build())
        .setTableRow(tableRow)
        .build();
  }
}
