# Air Quality Data Pipeline

## Task Overview
This task involves collecting and processing real-time air quality data from OpenWeatherMap
and sending it to AWS Kinesis for further analysis.

## Directory Structure


air-quality-data-pipeline/
│
├── ingestion/
│ ├── raw_data/
│ │ ├── batch/
│ │ │ ├── OpenWeatherMap/
│ │ │ │ ├── air_quality_2020_2025.json
│ │ │ │ └── data_quality_report_openweathermap.md
│ │ │ └── KAPSPARC/
│ │ └── real_time/
│ │ ├── raw_data/
│ │ ├── processed_data/
│ │ ├── scripts/
│ │ ├── logs/
│ │ ├── config.json
│ │ └── real_time_data_collection_log.txt
│ └── ...
└── ...

  
## Folder Descriptions

### `real_time/`  
This folder contains files and resources related to real-time data collection.  
  
- **raw_data/**: Stores raw JSON or CSV files received from real-time data sources.  
- **processed_data/**: Contains processed versions of the raw data.  
- **scripts/**: Includes Python scripts for data ingestion and processing.  
- **logs/**: Contains logs of the data collection process.  
- **config.json**: Configuration file for API keys, endpoints, and other settings.  
- **real_time_data_collection_log.txt**: Log file recording the status of data collection attempts.  
  
### `batch/`  
This folder contains historical batch data files.  
  
- **OpenWeatherMap/**: Contains historical air quality data from OpenWeatherMap.  
- **KAPSPARC/**: Contains historical air quality data from KAPSPARC.  
  
## Configuration File: `config.json`  
  
The `config.json` file includes the following settings:  
  
```json  
{  
    "openweathermap": {  
        "api_key": "YOUR_API_KEY",  
        "base_url": "http://api.openweathermap.org/data/2.5/air_pollution"  
    },  
    "aws": {  
        "region": "eu-north-1",  
        "kinesis_stream_name": "AirQualityStream",  
        "access_key": "YOUR_AWS_ACCESS_KEY",  
        "secret_key": "YOUR_AWS_SECRET_KEY"  
    },  
    "logging": {  
        "log_level": "INFO",  
        "log_file": "real_time_data_collection.log"  
    },  
    "data_collection": {  
        "city": "Riyadh",  
        "latitude": 24.7136,  
        "longitude": 46.6753,  
        "collection_interval": 300  
    }  
}  

Logging: real_time_data_collection_log.txt

The real_time_data_collection_log.txt file records the status of each data collection attempt, including:

    Timestamp: Date and time of the data collection.
    Request ID: Unique identifier for each Lambda invocation.
    Status: Success or error message.
    Response Code: HTTP response code from the API.
    Data Points Collected: Number of data points retrieved.
    Duration: Time taken for the data collection.
    Memory Usage: Memory allocated and used during execution.
    Error Messages: Any errors encountered during the process.

Example Log Entry

2025-03-13 10:00:00 - START RequestId: d167bd6a-aaf9-4038-a87f-b88c32de7ed9  
Status: Success  
Response Code: 200  
Data Points Collected: 5  
Duration: 579.92 ms  
Memory Size: 128 MB  
Max Memory Used: 82 MB  
Notes: Data collected successfully from OpenWeatherMap.  

### `Conclusion/`  

This task provides a comprehensive framework for collecting, processing, and analyzing real-time air quality data.
The structured approach ensures that all components are organized and easily manageable.