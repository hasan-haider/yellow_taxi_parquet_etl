# yellow_taxi_parquet_etl
parquet file loader to s3 and etl pipeline
# Author: Syed Hasan
Dataset: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page Yellow Taxi March 2022 parquet


Presentation Link: https://docs.google.com/presentation/d/1eqQam_PcUJrImCFWs1OnEfBrB8AZ8Y3ZIW0rj9uWeSE/edit?usp=sharing


Project Plan Link: https://docs.google.com/spreadsheets/d/1EJOXr3taY3n423pfoxz_PYW5se63j-Cbvoy77PrGnOI/edit?usp=sharing


Document Link: https://docs.google.com/document/d/18VVjqikpuoio7pjlFJK1Nqjjwzd9MwPHVQ9msK9TKXs/edit?usp=sharing
# Project Details:
## Task 1.	In this scenario, this data would be provided in single day batches based on the tpep_dropoff_datetime.
Split the parquet file based on this field into multiple files.

### Steps to implement:
1. Run requirements.txt file using pip to install dependencies.
2. Run file loader to load the data into S3 bucket into taxi-datasetetl/march/ folder.
3. Logs file will be generated.

## Task 2.	Implement a process that loads these files you’ve created in Task 1 into a database of your choice (SQL Server, MySQL, Postgres etc…)
Task 2 and 3 integrated together using AWS S3, glue, Redshift

## 3.	Test the quality of the data. Are there any suspicious data points? Can you track by storing these in the database as a separate table?

AWS Steps:
1. Create Redshift database
2. Add security rules for s3 and redshift to access in glue
3.Crawler setup for S3
4. Run glue job
- AWS Glue job to load data from s3 to redshift
- Transformation pipeline will load the error logs into the redshift log table

## 4.	Generate SQL query for the following,

### a.	For each day, show the average fare and total trip distance of all journeys.
SYNTAX:
select CAST(tpep_dropoff_datetime as DATE) as day,
AVG(total_amount) as average_fare, SUM(trip_distance) as total_trip_distance
FROM "dev"."public"."yellow_taxi_table"
group by CAST(tpep_dropoff_datetime as DATE);

### b.	Of the top ten taxis which have covered the greatest distance, return the duration of the longest taxi trip.
SYNTAX:
select top 1 DATEDIFF(second, tpep_pickup_datetime::timestamp, tpep_dropoff_datetime::timestamp) AS trip_duration_seconds
from 
(select * 
FROM processed_yellow_taxi_table
order by trip_distance desc limit 10)
order by trip_duration_seconds desc;

Thanks!
