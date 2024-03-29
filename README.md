# Dataprocessing challenge

This repository contains a solution to an interview challenge for the following problem:

A taxi company needs to extract insights from their data to improve their operations. To this end you must

1. Sketch an architecture on how to process new data batchwise and streams of data in the cloud.
2. Write a script to load the taxi data and join it with weather data
3. Perform data tests to evaluate the quality of the data

# Solution

1. Architecture

![Architecture](architecture_chart.jpg)

2. Data Ingestion, processing, testing, etc.

Data sources:

- taxi data (Jan 2016) (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- weather data (https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data?rvi=1)

Setup: `pip install -U -r requirements.txt`

Run: `python main.py`

```
Field: ('congestion_surcharge', Null) does not match expected field: ('congestion_surcharge', Float64)
Field: ('airport_fee', Null) does not match expected field: ('airport_fee', Float64)
congestion_surcharge has 100.0% of null values
airport_fee has 100.0% of null values
1181413 rows of passenger_count either below 1 or above 3!
422 had positive fare amounts but no passengers
humidity has 3.59% of null values
pressure has 2.3% of null values
temperature has 1.75% of null values
wind_speed has 1.75% of null values
Writing file output/taxi_data_final.parquet
Shape of final data: (10905067, 28)
```
