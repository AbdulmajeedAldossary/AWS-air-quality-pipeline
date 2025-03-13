# Data Quality Report for OpenWeatherMap Data

## Overview
This report outlines the quality checks performed on the batch data collected from various sources. The purpose of this report is to ensure the integrity, accuracy, and reliability of the data before it is used for analysis.

## Data Source
- **Source**: OpenWeatherMap  
- **Date Range**: Feburary 26, 2020 to March 03, 2025
- **Total Records**: 36,927



## Quality Checks Performed

### 1. Completeness
- **Description**: Checks for missing values in critical fields.
- **Findings**:
  - Total missing values: 0
  
### 2. Consistency
- **Description**: Checks for consistency in data formats and values.
- **Findings**:
  - Inconsistent timestamp values found: 0
 
### 3. Validity
- **Description**: Checks to ensure data values fall within expected ranges or categories.
- **Findings**:
  - Invalid values found: 4
- **Action Taken**: These invalid entries were excluded from the dataset to maintain data integrity.
    
### 4. Uniqueness
- **Description**: Checks for duplicate records in the dataset.
- **Findings**:
  - Total duplicate records: 0

### 5. Accuracy
- **Description**: Checks to compare data against known benchmarks or reference values.
- **Findings**:
  - No comparison was conducted as OpenWeatherMap began collecting data after the KAPSPARC data. However, the trends observed in both datasets are similar, indicating a consistent pattern in air quality measurements.
  - 
- **Action Taken**: Since no discrepancies were found due to the lack of overlapping data, further analysis will focus on monitoring future data from OpenWeatherMap to ensure alignment with KAPSPARC trends. Regular updates will be made to assess the accuracy of incoming data as it becomes available.

## Summary of Findings
- **Overall Data Quality**: Good
- **Recommendations**: Implement automated data validation checks during data ingestion to catch issues early.

## Conclusion
This data quality report provides an overview of the checks performed on the batch data. Ensuring high data quality is crucial for reliable analysis and decision-making.
