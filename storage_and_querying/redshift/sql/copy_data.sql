COPY air_quality_data
      FROM 's3://air-quality-processed-data-202502/processed-data/'  
      IAM_ROLE 'arn:aws:iam::123456789012:role/service-role/AmazonRedshift-CommandsAccessRole-20250223T173629'  
      FORMAT AS PARQUET