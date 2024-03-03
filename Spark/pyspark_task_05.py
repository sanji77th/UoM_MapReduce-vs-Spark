from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create a Spark session
spark = SparkSession.builder.appName("SecurityDelayAnalysis").getOrCreate()

# Define the S3 bucket path to your CSV file
s3_bucket_path = "s3://airlinedelaybucket/data/DelayedFlights-updated.csv"

# Read the CSV file into a Spark DataFrame
flights_df = spark.read.csv(s3_bucket_path, header=True, inferSchema=True)

# Perform the analysis for Task 05 (Year-wise security delay from 2003-2010)
year_wise_security_delay = flights_df.filter((flights_df['Year'] >= 2003) & (flights_df['Year'] <= 2010)) \
    .groupBy('Year') \
    .agg(sum('SecurityDelay').alias('TotalSecurityDelay'))

# Show the results
year_wise_security_delay.show()

# Stop the Spark session
spark.stop()
