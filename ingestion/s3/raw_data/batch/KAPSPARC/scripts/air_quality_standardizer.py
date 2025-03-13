import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, first, when, quarter, lit, concat, round
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

    # Handle the quarter column, standardize data, standardize units, and preserve periodicity for "monthly" or "daily"
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

    # Preserve quarter as NULL for "daily" or "monthly" periodicity if NULL in raw data, otherwise format for "quarterly" periodicity
    standardized_df = standardized_df.withColumn('quarter',
        when(col('periodicity').isin("daily", "monthly") & col('quarter').isNull(), lit(None))
        .when(col('periodicity') == "quarterly",
              when(col('quarter').isNotNull(), concat(lit('Q'), col('quarter').cast('string')))
              .otherwise(concat(lit('Q'), quarter(col('date')).cast('string'))))
        .otherwise(col('quarter'))  # Keep non-NULL quarter values for other periodicities
    )

    # Preserve periodicity as NULL for "monthly" or "daily" if NULL in raw data, otherwise keep the value
    standardized_df = standardized_df.withColumn('periodicity',
        when(col('periodicity').isin("monthly", "daily") & col('periodicity').isNull(), lit(None))
        .otherwise(col('periodicity'))
    )

    # Standardize units for pollutants and ensure consistent unit column, then round to 2 decimal places
    standardized_df = standardized_df.withColumn('value_standardized',
        when((col('component') == 'CO') & (col('unit') == 'ppm'), round(col('value') * 1145, 2))  # Convert ppm to µg/m³ and round to 2 decimals
        .when((col('component') == 'CO') & (col('unit') == 'ppb'), round(col('value') * 1.145, 2))  # Convert ppb to µg/m³ and round to 2 decimals
        .when((col('component') == 'SO2') & (col('unit') == 'ppm'), round(col('value') * 2620, 2))  # Convert ppm to µg/m³ and round to 2 decimals
        .when((col('component') == 'SO2') & (col('unit') == 'ppb'), round(col('value') * 2.62, 2))  # Convert ppb to µg/m³ and round to 2 decimals
        .when((col('component') == 'NO2') & (col('unit') == 'ppm'), round(col('value') * 1880, 2))  # Convert ppm to µg/m³ and round to 2 decimals
        .when((col('component') == 'NO2') & (col('unit') == 'ppb'), round(col('value') * 1.88, 2))  # Convert ppb to µg/m³ and round to 2 decimals
        .when((col('component') == 'O3') & (col('unit') == 'ppm'), round(col('value') * 1960, 2))  # Convert ppm to µg/m³ and round to 2 decimals
        .when((col('component') == 'O3') & (col('unit') == 'ppb'), round(col('value') * 1.96, 2))  # Convert ppb to µg/m³ and round to 2 decimals
        .when((col('component') == 'PM10') & (col('unit') == 'µg/m³'), round(col('value'), 2))  # Keep PM10 in µg/m³ and round to 2 decimals
        .otherwise(round(col('value'), 2))  # Assume µg/m³ if unit is missing or unknown, round to 2 decimals
    ).withColumn('unit',
        lit('µg/m³')  # Set all units to µg/m³ to match OpenWeatherMap standardization
    )

    # Print sample data after transformations
    print("\nSample data after standardization:")
    standardized_df.show(5)

    # Pivot the data using standardized values
    pivoted_df = standardized_df.groupBy(
        'date',
        'city',
        'station',
        'year',
        'month',
        'unit',
        'indicator',
        'periodicity',
        'quarter'  # Include periodicity and quarter in groupBy
    ).pivot('component', ['CO', 'SO2', 'NO2', 'O3', 'PM10']) \
     .agg(first('value_standardized'))  # Use value_standardized for consistency

    # Print pivoted data sample (no NULL filtering)
    print("\nSample pivoted data:")
    pivoted_df.show(5)

    # Ensure partitioning columns are non-NULL and correct types before writing
    final_df = pivoted_df.na.drop(subset=["year", "month", "city", "station"]) \
                         .withColumn('year', col('year').cast('int')) \
                         .withColumn('month', col('month').cast('int')) \
                         .withColumn('city', col('city').cast('string')) \
                         .withColumn('station', col('station').cast('string'))

    # Write the data with partitioning (no NULL filtering)
    output_path = "s3://air-quality-processed-data-202502/processed-data"

    print(f"\nWriting data to: {output_path}")
    print("Partitioning by: year, month, city, station")

    final_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "city", "station") \
        .parquet(output_path)

    # Print final statistics
    print("\nFinal Statistics:")
    print(f"Total records written: {final_df.count()}")

    print("\nDistribution by Year and Month:")
    final_df.groupBy("year", "month").count().orderBy("year", "month").show()

    print("\nDistribution by Quarter:")
    final_df.groupBy("quarter", "periodicity").count().show()

    # Additional analysis
    print("\nDistribution by Station:")
    final_df.groupBy("station").count().orderBy(col("count").desc()).show()

    print("\nDate Range of Data:")
    final_df.agg(
        {"date": "min", "date": "max"}
    ).show()

except Exception as e:
    print(f"An error occurred: {str(e)}")
    raise e

finally:
    job.commit()
    print("Job completed")