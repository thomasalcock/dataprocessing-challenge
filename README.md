# dataprocessing-challenge
Solution to the data processing challenge 

1. Architecture

![Architecture](architecture_chart.jpg)

2. Data Ingestion

Data sources:
    - taxi data
    - weather data


Setup: `pip install -U -r requirements.txt`

Run: `python main.py`

```
    Field: ('congestion_surcharge', Null) does not match expected field: ('congestion_surcharge', Float64)
    Field: ('airport_fee', Null) does not match expected field: ('airport_fee', Float64)
    congestion_surcharge has 100.0% of null values
    airport_fee has 100.0% of null values
    humidity has 3.59% of null values
    pressure has 2.3% of null values
    19 rows of pressure either below 850 or above 1050!
    temperature has 1.75% of null values
    wind_speed has 1.75% of null values
    output does not exist. Creating directory
    Writing file output/taxi_data_final.parquet
    Shape of final data: (10905067, 28)
```