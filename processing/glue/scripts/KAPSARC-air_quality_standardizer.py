import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, first, when, quarter
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
        .json("s3://air-quality-raw-data-202502/batch-data/air-quality.json")

    print(f"Total records before processing: {df.count()}")
    print("\nInitial Schema:")
    df.printSchema()

    # Print distribution of periodicity and quarter
    print("\nDistribution by periodicity:")
    df.groupBy("periodicity").count().show()
    print("\nDistribution by quarter:")
    df.groupBy("quarter").count().show()

    # Handle the quarter column and standardize data
    standardized_df = df.select(
        'date',
        'city',
        'station',
        'component',
        col('value').cast('double').alias('value'),
        'unit',
        'indicator',
        'periodicity',
        'quarter'  # Include quarter column
    ).withColumn('year', year(col('date'))) \
     .withColumn('month', month(col('date')))

    # Print sample data after transformations
    print("\nSample data after standardization:")
    standardized_df.show(5)

    # Pivot the data
    pivoted_df = standardized_df.groupBy(
        'date',
        'city',
        'station',
        'year',
        'month',
        'unit',
        'indicator',
        'periodicity',
        'quarter'  # Include quarter in groupBy
    ).pivot('component', ['CO', 'SO2', 'NO2', 'O3', 'PM10']) \
     .agg(first('value'))

    # Print pivoted data sample
    print("\nSample pivoted data:")
    pivoted_df.show(5)

    # Write the data with partitioning
    output_path = "s3://air-quality-processed-data-202502/processed-data"

    print(f"\nWriting data to: {output_path}")
    print("Partitioning by: year, month, city, station")

    pivoted_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "city", "station") \
        .parquet(output_path)

    # Print final statistics
    print("\nFinal Statistics:")
    print(f"Total records written: {pivoted_df.count()}")

    print("\nDistribution by Year and Month:")
    pivoted_df.groupBy("year", "month").count().orderBy("year", "month").show()

    print("\nDistribution by Quarter:")
    pivoted_df.groupBy("quarter", "periodicity").count().show()

    # Additional analysis
    print("\nDistribution by Station:")
    pivoted_df.groupBy("station").count().orderBy(col("count").desc()).show()

    print("\nDate Range of Data:")
    pivoted_df.agg(
        {"date": "min", "date": "max"}
    ).show()

except Exception as e:
    print(f"An error occurred: {str(e)}")
    raise e

finally:
    job.commit()
    print("Job completed")