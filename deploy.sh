#!/usr/bin/env bash
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -x
## update following export params
export PROJECT_ID=$(gcloud config get-value project)
export GCS_BUCKET_NAME=${PROJECT_ID}-image-files
export BQ_DATASET_NAME="image_data"
export FEATURE_TYPE="{\"featureConfig\":[{\"type\":\"LABEL_DETECTION\"}\,{\"type\":\"FACE_DETECTION\"}\,{\"type\":\"LANDMARK_DETECTION\"}\,{\"type\":\"LOGO_DETECTION\"}\,{\"type\":\"TEXT_DETECTION\"}\,{\"type\":\"DOCUMENT_TEXT_DETECTION\"}\,{\"type\":\"SAFE_SEARCH_DETECTION\"}\,{\"type\":\"IMAGE_PROPERTIES\"}\,{\"type\":\"CROP_HINTS\"}\,{\"type\":\"WEB_DETECTION\"}\,{\"type\":\"PRODUCT_SEARCH\"}\,{\"type\":\"OBJECT_LOCALIZATION\"}]}"
export SELECTED_COLUMNS="{\"selectedColumns\":[{\"labelAnnotations\":\"description,score\"}]}"
## enable apis
gcloud services enable dataflow
gcloud services enable bigquery
gcloud services enable storage_component
gcloud services enable vision.googleapis.com

gcloud builds submit . --config cloudbuild.yaml --substitutions ^~^_GCS_BUCKET_NAME=$GCS_BUCKET_NAME~_BQ_DATASET_NAME=$BQ_DATASET_NAME~_FEATURE_TYPE=$FEATURE_TYPE~_SELECTED_COLUMNS=$SELECTED_COLUMNS~_API_KEY=$(gcloud auth print-access-token)