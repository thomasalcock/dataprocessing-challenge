from collections import OrderedDict
from polars import Int64, Float64, Utf8, Datetime

# all schemas are defined as ordered dicts, because polars returns the schema as an ordered dict
# some columns may have null values in the actual data, but here the data types indicted the *intended* type
expected_taxi_data_schema = OrderedDict(
    [
        ("VendorID", Int64),
        ("tpep_pickup_datetime", Datetime(time_unit="ns", time_zone=None)),
        ("tpep_dropoff_datetime", Datetime(time_unit="ns", time_zone=None)),
        ("passenger_count", Int64),
        ("trip_distance", Float64),
        ("RatecodeID", Int64),
        ("store_and_fwd_flag", Utf8),
        ("PULocationID", Int64),
        ("DOLocationID", Int64),
        ("payment_type", Int64),
        ("fare_amount", Float64),
        ("extra", Float64),
        ("mta_tax", Float64),
        ("tip_amount", Float64),
        ("tolls_amount", Float64),
        ("improvement_surcharge", Float64),
        ("total_amount", Float64),
        ("congestion_surcharge", Float64),
        ("airport_fee", Float64),
    ]
)

# all weather data sources store their data in a wide format
# with locations as columns and the value in each column equal to
# the
expected_weather_data_schema = OrderedDict(
    [
        ("datetime", Utf8),
        ("Vancouver", Float64),
        ("Portland", Float64),
        ("San Francisco", Float64),
        ("Seattle", Float64),
        ("Los Angeles", Float64),
        ("San Diego", Float64),
        ("Las Vegas", Float64),
        ("Phoenix", Float64),
        ("Albuquerque", Float64),
        ("Denver", Float64),
        ("San Antonio", Float64),
        ("Dallas", Float64),
        ("Houston", Float64),
        ("Kansas City", Float64),
        ("Minneapolis", Float64),
        ("Saint Louis", Float64),
        ("Chicago", Float64),
        ("Nashville", Float64),
        ("Indianapolis", Float64),
        ("Atlanta", Float64),
        ("Detroit", Float64),
        ("Jacksonville", Float64),
        ("Charlotte", Float64),
        ("Miami", Float64),
        ("Pittsburgh", Float64),
        ("Toronto", Float64),
        ("Philadelphia", Float64),
        ("New York", Float64),
        ("Montreal", Float64),
        ("Boston", Float64),
        ("Beersheba", Float64),
        ("Tel Aviv District", Float64),
        ("Eilat", Float64),
        ("Haifa", Float64),
        ("Nahariyya", Float64),
        ("Jerusalem", Float64),
    ]
)
