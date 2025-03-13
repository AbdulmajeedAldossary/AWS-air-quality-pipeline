# Air Quality Data Pipeline Project

## Overview
The Air Quality Data Pipeline Project is designed to process air quality data using both batch and real-time methods. This project utilizes KAPSPARC as a source for batch data and OpenWeatherMap for both batch and real-time data, covering the period from 2019 to 2025.

### Architecture Diagram
Below is the architecture diagram that illustrates the components and flow of the data pipeline:

![Architecture Diagram](/architecture_diagram/architecture_diagram.jpg)

## Project Goals

### Main Objectives:
- Build a scalable data pipeline to ingest, process, and analyze air quality data for Riyadh, focusing on eight key pollutants: CO, SO2, NO2, O3, PM10, NO, PM2.5, and NH3.
- Provide actionable insights into air quality trends, pollution hotspots, and seasonal variations using a structured 8-step analysis process.
- Enable visualization of air quality metrics through interactive dashboards for stakeholders like environmental agencies, researchers, and policymakers.

### Problems It Aims to Solve:
- Lack of centralized, real-time air quality monitoring and analysis for Riyadh.
- Difficulty in identifying trends and anomalies in air quality data due to scattered and unprocessed datasets.
- Need for a structured approach to analyze and visualize air quality data for better decision-making.

## Technologies Used

### AWS Services:
- **Amazon S3:** For storing raw and processed data (data lake).
- **AWS Kinesis Data Streams:** For real-time data ingestion.
- **AWS Glue:** For batch ETL (Extract, Transform, Load) processing.
- **AWS Lambda:** For real-time data transformation.
- **Amazon Redshift:** For storing processed data in a data warehouse.
- **Amazon QuickSight:** For creating dashboards and visualizations.
- **Amazon CloudWatch:** For monitoring and logging pipeline activities.

### Programming Languages:
- **Python:** For scripting, data simulation, and Lambda functions.
- **SQL:** For querying and modeling data in Redshift.

### Frameworks and Libraries:
- **Boto3:** AWS SDK for Python to interact with AWS services.
- **Pandas:** For data manipulation and analysis.
- **Matplotlib/Seaborn:** For local data visualization during testing.
- **APIs:** OpenWeatherMap API and OpenAQ API for fetching air quality data.

## Data Processing Details

### Steps in the Data Processing Pipeline:
1. **Data Ingestion:**
   - Real-time data is ingested using AWS Kinesis Data Streams from APIs like OpenWeatherMap or simulated data.
   - Batch data (historical air quality data) is uploaded to Amazon S3 in CSV/JSON format.

2. **Data Transformation:**
   - **AWS Glue** processes batch data:
     - Cleans missing or invalid values.
     - Converts timestamps to a unified format.
     - Aggregates data by region, pollutant type, and time intervals.
   - **AWS Lambda** processes real-time data:
     - Parses JSON payloads from Kinesis.
     - Validates pollutant readings and adds metadata (e.g., region, timestamp).

3. **Data Storage:**
   - Raw data is stored in S3 (data lake).
   - Processed data is loaded into Amazon Redshift using COPY commands or Glue jobs.
   - A star schema is designed in Redshift for efficient querying.

4. **Data Analytics and Visualization:**
   - Amazon QuickSight connects to Redshift to create dashboards showing:
     - Trends in pollutant levels (e.g., PM2.5, CO, NO2).
     - Anomalies or spikes in air quality metrics.
     - Comparisons across regions or time periods.

## Insights and Visualization

### Dashboard Access
You can view the interactive dashboards created using Amazon QuickSight at the following link:

[View Dashboard](https://eu-north-1.quicksight.aws.amazon.com/sn/accounts/796973475601/dashboards/fde8807e-f694-4c4c-91c8-723808df2453?directory_alias=AbdulmajeedAldossary)

### 8-Step Analysis Process:
1. **Data Cleaning:**
   - Addressed missing values and outliers for all eight pollutants.
   - Used a two-phase imputation approach to respect station-specific patterns while ensuring data integrity.

2. **Descriptive Statistics:**
   - Generated statistical summaries (mean, median, standard deviation) for each pollutant.
   - Established baseline pollution profiles for Riyadh.

3. **Temporal Analysis:**
   - Analyzed how pollutant levels varied over time (hourly, daily, monthly).
   - Identified seasonal pollution drivers, such as increased PM10 levels during dust storms.

4. **Spatial Analysis:**
   - Mapped pollutant levels across monitoring stations to identify hotspots.
   - Used pollutant ratios to distinguish between emission sources (e.g., traffic vs. industrial).

5. **Threshold Analysis:**
   - Compared pollutant levels against WHO and local health standards.
   - Highlighted pollutants exceeding safe thresholds, such as PM2.5 and NO2.

6. **Peak Analysis:**
   - Identified extreme pollution events, their duration, and contextual factors (e.g., weather conditions, traffic patterns).
   - Differentiated between short-term spikes and sustained pollution episodes.

7. **Correlation Analysis:**
   - Examined relationships between pollutants (e.g., NO2 and O3).
   - Identified shared sources and potential chemical interactions in the atmosphere.

8. **Seasonal Analysis:**
   - Quantified seasonal patterns for each pollutant.
   - Highlighted the impact of weather and human activities on air quality.

### Visualizations:
- Dashboards in QuickSight displayed:
  - Time-series graphs of pollutant levels.
  - Heatmaps showing regional air quality variations.
  - Alerts for pollutants exceeding thresholds.

## Challenges Faced

### Challenges:
- **Data Quality Issues:**
  - Missing or inconsistent values in batch datasets.
  - Real-time data streams occasionally contain invalid readings.
  - **Solution:** Implemented data validation and cleaning steps in Glue and Lambda functions.


- **Cost Management:**
  - Running multiple AWS services simultaneously increased costs during testing.
  - **Solution:** Used AWS Free Tier where possible and implemented cost optimization strategies (e.g., S3 lifecycle policies, reserved instances for Redshift).

## Future Work

### Planned Enhancements:
- **Automation:** Integrate AWS Step Functions to automate the pipeline workflows.
- **Machine Learning Integration:** Use AWS SageMaker to predict future air quality trends based on historical data and implement anomaly detection models for real-time data.
- **Geospatial Analysis:** Add geospatial visualizations to show pollutant levels on a map.
- **Multi-Region Support:** Expand the pipeline to ingest and analyze data from multiple countries or regions.

## Conclusion
This project demonstrates the ability to build a robust data pipeline for air quality monitoring, integrating multiple data sources and processing methods to provide valuable insights into air quality trends over time.
