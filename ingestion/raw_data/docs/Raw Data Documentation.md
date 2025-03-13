# Raw Data Documentation

## Overview
This folder contains the raw data used for analyzing air quality over the last six years, from 2019 to the present. The data is sourced from various platforms to ensure comprehensive coverage and accuracy in our analysis.

## Data Sources

### Batch Data
- **KAPSPARC**: 
  - Batch data is collected from KAPSPARC, which provides historical air quality measurements. This data is aggregated and stored for analysis.

**Batch Data**: Stored in the `/KAPSPARC` subfolder, containing historical data files.  
  - **Files**:  
    - `air-quality.json`: Contains the raw air quality data collected from KAPSPARC.  
    - `metadata_air_quality.json`: Provides metadata describing the structure and content of the KAPSPARC raw data.
  
- **OpenWeatherMap**: 
  - Batch data is also sourced from OpenWeatherMap, which offers historical weather data that complements air quality measurements.

### Real-Time Data
- **OpenWeatherMap**: 
  - Real-time air quality data is obtained from OpenWeatherMap. This data is crucial for monitoring current air quality conditions and trends.

## Purpose
The primary purpose of this raw data is to conduct a thorough analysis of air quality trends and patterns over the past six years. This analysis aims to:
- Identify seasonal variations in air quality.
- Evaluate compliance with air quality standards and guidelines.
- Provide insights for policymakers and stakeholders to improve air quality management.


## Data Structure  
The raw data is organized into two main categories:  
- **Batch Data**: Stored in the `/OpenWeatherMap` subfolder, containing historical data files.  
  - **Files**:  
    - `air_quality_2020_2025.json`: Contains the raw air quality data collected from OpenWeatherMap.  
    - `metadata_air_quality_2020_2025.json`: Provides metadata describing the structure and content of the raw data.  
    
- **Real-Time Data**: Stored in the `/real_time` subfolder, containing current data files.  
  - **Files**:  
    - `real_time_data_collection_log.txt`: Logs the status of each data collection attempt, including timestamps, request IDs, and success/error messages.  
    - `config.json`: Configuration file containing API keys, endpoints, and other settings necessary for data collection.  
  
## Logging  
The logging mechanism captures the status of data collection attempts, including:  
- **Timestamp**: Date and time of each data collection attempt.  
- **Request ID**: Unique identifier for each invocation of the data collection function.  
- **Status**: Indicates whether the data collection was successful or failed.  
- **Response Code**: HTTP response code received from the API.  
- **Data Points Collected**: Number of data points retrieved during each successful fetch.  
- **Duration**: Time taken for each data collection attempt.  
- **Memory Usage**: Memory allocated and used during execution.  
- **Error Messages**: Any errors encountered during the data collection process.  
  
## Conclusion  
This documentation provides a comprehensive overview of the raw data used for air quality analysis, including its sources, structure, and logging practices. This structured approach ensures that all components are organized and easily manageable, facilitating effective analysis and reporting.