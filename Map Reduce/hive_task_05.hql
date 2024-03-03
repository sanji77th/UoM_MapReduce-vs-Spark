-- security_delay_analysis.hql

-- Create an external table pointing to the S3 CSV file
CREATE EXTERNAL TABLE flights_task_05 (
  Year INT,
  Month INT,
  DayofMonth INT,
  DayOfWeek INT,
  DepTime INT,
  CRSDepTime INT,
  ArrTime INT,
  CRSArrTime INT,
  UniqueCarrier STRING,
  FlightNum INT,
  TailNum STRING,
  ActualElapsedTime INT,
  CRSElapsedTime INT,
  AirTime INT,
  ArrDelay INT,
  DepDelay INT,
  Origin STRING,
  Dest STRING,
  Distance INT,
  TaxiIn INT,
  TaxiOut INT,
  Cancelled STRING,
  CancellationCode STRING,
  Diverted STRING,
  CarrierDelay INT,
  WeatherDelay INT,
  NASDelay INT,
  SecurityDelay INT,
  LateAircraftDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://airlinedelaybucket/data/';

-- Perform the analysis and store the result in a table
CREATE TABLE hive_task_05 AS
SELECT
  Year,
  UniqueCarrier,
  SUM(SecurityDelay) AS TotalSecurityDelay
FROM flights_task_05
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year, UniqueCarrier;

SELECT * FROM hive_task_05;
