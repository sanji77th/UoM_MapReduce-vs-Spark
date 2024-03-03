from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create a Spark session
spark = SparkSession.builder.appName("CarrierDelayAnalysis").getOrCreate()

# Define the S3 bucket path to your CSV file
s3_bucket_path = "s3://airlinedelaybucket/data/DelayedFlights-updated.csv"

# Read the CSV file into a Spark DataFrame
flights_df = spark.read.csv(s3_bucket_path, header=True, inferSchema=True)

# Perform the analysis for each iteration
for year in range(2003, 2008):
    year_wise_carrier_delay = flights_df.filter((flights_df['Year'] == year)) \
        .groupBy('Year', 'UniqueCarrier') \
        .agg(sum('CarrierDelay').alias('TotalCarrierDelay'))

    # Show the results for each iteration
    print(f"Year {year} - Carrier Delay Analysis:")
    year_wise_carrier_delay.show()

# Stop the Spark session
spark.stop()
