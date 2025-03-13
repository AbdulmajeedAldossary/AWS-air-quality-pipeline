# S3 Configuration and Folder Documentation

## S3 Bucket Information
- An S3 bucket named `air-quality-raw-data-202502` was created for KAPSPARC raw data storage.
- An S3 bucket named `openweathermap-air-quality-processed-data-202502` was created for OpenWeatherMap raw data storage.
-

## Overview
The `s3_config.json` file contains configuration settings related to the usage of Amazon S3 within the air quality data pipeline project. This file helps manage S3 bucket details, access permissions, and other relevant settings for data storage and retrieval.


## KAPSPARC Raw Data Storage

```json
{
    "s3": {
        "bucket_names": {
            "raw_data_bucket": "air-quality-raw-data-202502"
        
          
        },
        "region": "eu-north-1",
        "file_paths": {
            "raw_data_path": "raw_data/"
             
        }
    },
    "aws": {
        "access_key": "",   
        "secret_key": ""   
    }
     
}

```

## OpenWeatherMap Raw Data Storage

```json
{
    "s3": {
        "bucket_names": {
            "raw_data_bucket": "openweathermap-air-quality-processed-data-202502"
        
          
        },
        "region": "eu-north-1",
        "file_paths": {
            "raw_data_path": "raw_data/"
             
        }
    },
    "aws": {
        "access_key": "",   
        "secret_key": ""   
    }
     
}

```