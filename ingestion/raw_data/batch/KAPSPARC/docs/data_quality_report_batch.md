# Data Quality Report for Batch Data

## Overview
This report outlines the quality checks performed on the batch data collected from various sources. The purpose of this report is to ensure the integrity, accuracy, and reliability of the data before it is used for analysis.

## Data Source
- **Source**: KAPSPARC  
- **Date Range**: January 1, 2019 to Feburary 26, 2020
- **Total Records**: 17,299

## Quality Checks Performed

### 1. Completeness
- **Description**: Checks for missing values in critical fields.
- **Findings**:
  - Total missing values: 1,6879
  - `quarter`: 16879 missing values
  - Fields with missing values:
    - `quarter`: 16879 missing values
    -
- **Action Taken**: The missing `quarter` values were noted as part of the data reporting structure. Since the absence of quarter does not affect the daily data analysis, no imputation was performed for this field.

### 2. Consistency
- **Description**: Checks for consistency in data formats and values.
- **Findings**:
  - Inconsistent formats found: 0
 
### 3. Validity
- **Description**: Checks to ensure data values fall within expected ranges or categories.
- **Findings**:
  - Invalid values found: 0
    
### 4. Uniqueness
- **Description**: Checks for duplicate records in the dataset.
- **Findings**:
  - Total duplicate records: 1
  - Fields checked for uniqueness: `date`, `station`
- **Action Taken**: Duplicate records were removed based on the `date` and `station` .

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





