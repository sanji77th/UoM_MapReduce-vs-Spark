from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create a Spark session
spark = SparkSession.builder.appName("NASDelayAnalysis").getOrCreate()

# Define the S3 bucket path to your CSV file
s3_bucket_path = "s3://airlinedelaybucket/data/DelayedFlights-updated.csv"

# Read the CSV file into a Spark DataFrame
flights_df = spark.read.csv(s3_bucket_path, header=True, inferSchema=True)

# Perform the analysis for NAS delay
year_wise_nas_delay = flights_df.filter((flights_df['Year'] >= 2003) & (flights_df['Year'] <= 2010)) \
    .groupBy('Year') \
    .agg(sum('NASDelay').alias('TotalNASDelay'))

# Show the results
year_wise_nas_delay.show()

# Stop the Spark session
spark.stop()
