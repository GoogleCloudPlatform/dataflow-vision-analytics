# Processing Large Scale Image Files Using Dataflow and Vision API

This repo contains a reference implementations for a series of video analytics solutions by using Dataflow & Video AI. The goal is to provide an easy to use end to end solution to process large scale image data by bringing multiple data streams together to drive insight using Vision  AI.


# Reference Architecture

![ref_arch](diagram/vision_api_ref_arch.png)


# Setup a Customer Demo

1. Clone the repo: 

```
glogin 
git clone sso://user/masudhasan/df-vision-api
cd dataflow-vision-api
```

2. Run the deploy script. 

```
gcloud init
gcloud config set project [project_id]
sh deploy.sh
```

3. Validate the BigQuery Table. Please allow 5-8 mins for dataflow process to complete.


```
// validate raw json mode

CREATE TEMP FUNCTION json2array(json STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  return JSON.parse(json).map(x=>JSON.stringify(x));
"""; 

SELECT * EXCEPT(array_commits),
  ARRAY(SELECT JSON_EXTRACT_SCALAR(x, '$.description') FROM UNNEST(array_commits) x) descriptions,
  ARRAY(SELECT JSON_EXTRACT_SCALAR(x, '$.score') FROM UNNEST(array_commits) x) score
FROM (
SELECT file_name, json2array(JSON_EXTRACT(raw_json_response, '$')) array_commits
FROM `[project_id].image_data.VISION_API_FINDINGS_RAW_JSON` 
WHERE feature_type='labelAnnotations')

// validate select column mode
SELECT file_name, count(description) FROM `[project_id].image_data.VISION_API_FINDINGS_LABEL_DETECTION` group by file_name
``` 
# To Learn More About This Solution
# Before Start
This pipeline operates in two different modes:  

1. By default LABEL_DETECTION is used as feature type. Results are stored in a BQ table in raw JSON format.  Please refer to the screen shot below:

 ![default_table_schema](diagram/default_table_schema.png)

 ![sample_data](diagram/default_table_data.png)

2.  Alternatively, you can override default feature type by specifying a JSON config like below to trigger the pipeline. Please refer to the [sample script](./src/main/resources/sample_scripts/runPipelineDefaultMode.sh) provided as part fo the repo.   
 

```
For LANDMARK_DETECTION, you can use   
{\"featureConfig\":[{\"type\":\"LANDMARK_DETECTION\"}]}	
```
```
For ALL feature types, you can use  
featureType={\"featureConfig\":[{\"type\":\"LABEL_DETECTION\"},{\"type\":\"FACE_DETECTION\"},{\"type\":\"LANDMARK_DETECTION\"},{\"type\":\"LOGO_DETECTION\"},{\"type\":\"LOGO_DETECTION\"},{\"type\":\"TEXT_DETECTION\"},{\"type\":\"DOCUMENT_TEXT_DETECTION\"},{\"type\":\"SAFE_SEARCH_DETECTION\"},{\"type\":\"IMAGE_PROPERTIES\"},{\"type\":\"CROP_HINTS\"},{\"type\":\"WEB_DETECTION\"},{\"type\":\"PRODUCT_SEARCH\"},{\"type\":\"OBJECT_LOCALIZATION\"}]}
``` 

Lastly, you can specify a list of columns to override the default JSON output mode in BQ table. For example if you pass below parameter, BQ table will only contain the columns you specified. 

```{\"featureConfig\":[{\"type\":\"LANDMARK_DETECTION\"}]}
{\"selectedColumns\":[{\"landmarkAnnotations\":\"description,score\"}]}
```

As output to BQ, you will see a table created like below:

![selected_column_mode_](diagram/selected_columns.png)

## Getting Started

````
gcloud services enable dataflow
gcloud services enable bigquery
gcloud services enable storage_component
gcloud services enable vision.googleapis.com
````

### Creating a BigQuery Dataset

```
bq --location=US mk -d \ 
--description "Vision API Results" \ 
VISION_API_DATASET
```

### Creating a GCS Bucket and Upload Image Files 

To create a new bucket in a specific region, please follow this [link](https://cloud.google.com/storage/docs/creating-buckets).

* Please note default script below uses sample images so this step is not necessary if you would just like to try it out quickly.

### Run the Pipeline
Modify this script to add your Project, BigQuery Dataset and GCS path for the image files and run it.

```
sh runPipelineDefaultMode.sh
```
### Validate Dataflow & BigQuery
 
![df_1](diagram/df_1.png) 
![df_2](diagram/df_2.png) 

# Build & Run
To Build 

```
gradle build -DmainClass=com.google.solutions.ml.api.vision.VisionTextToBigQueryStreaming  
```

To Run with default mode: 

```
gradle run -DmainClass=com.google.solutions.ml.api.vision.VisionTextToBigQueryStreaming -Pargs=" --streaming --project=<project_id> --runner=DataflowRunner --inputFilePattern=gs://<bucket>/*.* --datasetName=<BQ_Dataset> --visionApiProjectId=<project_id_where_vision_api_enabled> --enableStreamingEngine"
```
# Creating a Docker Image For Dataflow Dynamic Template
Create the image using Jib

```
gradle jib --image=gcr.io/[project_id]/df-vision-api-pipeline:latest -DmainClass=com.google.solutions.ml.api.vision.VisionTextToBigQueryStreaming 
```

Update the [spec file](./src/main/resources/dynamic_template_vision_api.json) and store it in a GCS bucket. Please use the GCS path as BUCKET_SPEC in the sample scripts.   

# Batch Size

Please check out the [quota and limit](https://cloud.google.com/vision/quotas) for Vision API Image request. 
Pipeline is defaulted to process 16 images/request in parallel. 
For large number of files, you may have to increase  1800 request/minute quota in your project.

# Known Issues
* In selected column mode, a repeated field is not allowed except at the last position of a [field mask](https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/FieldMask)

