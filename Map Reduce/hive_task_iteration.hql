-- carrier_delay_analysis.hql

-- Create an external table pointing to the S3 CSV file
CREATE EXTERNAL TABLE IF NOT EXISTS flights_task_iteration (
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




CREATE TABLE IF NOT EXISTS hive_task_iteration AS
SELECT
  Year,
  UniqueCarrier,
  SUM(CarrierDelay) AS TotalCarrierDelay
FROM flights_task_iteration
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year, UniqueCarrier;
  

  

DROP TABLE IF EXISTS flights_task_iteration;
DROP TABLE IF EXISTS hive_task_iteration;

