import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, explode
from pyspark.sql.types import *

# Initialize contexts
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Read the JSON data with multiLine option
    df = spark.read \
        .option("multiline", "true") \
        .option("mode", "PERMISSIVE") \
        .json("s3://air-quality-raw-data-202502/openweathermap-batch-data/historical_air_quality_data.json")

    print(f"Total records before processing: {df.count()}")
    print("\nInitial Schema:")
    df.printSchema()
    df.show(5, truncate=False)  # Show initial data

    # Explode the 'list' array to flatten the structure
    exploded_df = df.select(
        col("coord.lon").alias("longitude"),
        col("coord.lat").alias("latitude"),
        explode(col("list")).alias("list_item")
    )

    print("\nExploded Data:")
    exploded_df.show(5, truncate=False)  # Show exploded data

    # Extract relevant fields from the exploded 'list_item'
    standardized_df = exploded_df.select(
        "longitude",
        "latitude",
        col("list_item.main.aqi").cast("int").alias("aqi"),
        col("list_item.components.co").cast("double").alias("co"),
        col("list_item.components.no").cast("double").alias("no"),
        col("list_item.components.no2").cast("double").alias("no2"),
        col("list_item.components.o3").cast("double").alias("o3"),
        col("list_item.components.so2").cast("double").alias("so2"),
        col("list_item.components.pm2_5").cast("double").alias("pm2_5"),
        col("list_item.components.pm10").cast("double").alias("pm10"),
        col("list_item.components.nh3").cast("double").alias("nh3"),
        col("list_item.dt").cast("int").alias("timestamp")  # Ensure it's int
    ).filter(col('timestamp').isNotNull())  # Filter out null timestamps

    # Check the timestamp values
    print("\nTimestamp Values:")
    standardized_df.select("timestamp").show(5)

    # Convert timestamp to date without dividing by 1000
    standardized_df = standardized_df.withColumn('date', col('timestamp').cast("timestamp")) \
                                       .withColumn('year', year(col('date'))) \
                                       .withColumn('month', month(col('date')))

    # Print sample data after transformations
    print("\nSample data after standardization:")
    standardized_df.show(5)

    # Write the data with partitioning
    output_path = "s3://air-quality-processed-data-202502/processed-data/openweathermap"

    print(f"\nWriting data to: {output_path}")
    print("Partitioning by: year, month")

    standardized_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)

    # Print final statistics
    print("\nFinal Statistics:")
    print(f"Total records written: {standardized_df.count()}")

    print("\nDistribution by Year and Month:")
    standardized_df.groupBy("year", "month").count().orderBy("year", "month").show()

except Exception as e:
    print(f"An error occurred: {str(e)}")
    raise e

finally:
    job.commit()
    print("Job completed")