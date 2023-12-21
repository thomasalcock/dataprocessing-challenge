import os
import polars as pl
from utils import (
    compare_schemas,
    prepare_weather_data_source,
    extract_datetime_information,
    print_df,
    check_for_nulls,
    check_range,
)
from metadata import expected_taxi_data_schema, expected_weather_data_schema
from config import ConfigClass

if __name__ == "__main__":
    # preparation of taxi data
    Config = ConfigClass()
    taxi_df = pl.read_parquet(Config.taxi_files)

    compare_schemas(expected_taxi_data_schema, taxi_df.schema)

    check_for_nulls(taxi_df, Config.null_threshold)

    # year, month, day and hours are extracted to make the data joinable with the weather data below
    taxi_df = extract_datetime_information(taxi_df, "tpep_pickup_datetime")

    # assuming a cab has 3 seats, this is the max passenger count
    # empty rides should not show up
    check_range(taxi_df, "passenger_count", min_value=1, max_value=3)

    # these may be anomalies or some kind of upfront payment before the actual trip happens
    fares_but_no_passengers = taxi_df.filter(
        (pl.col("passenger_count") == 0) & (pl.col("fare_amount") > 0)
    )

    n_fares_but_no_passengers = fares_but_no_passengers.shape[0]

    if n_fares_but_no_passengers > 0:
        print(
            f"{n_fares_but_no_passengers} trips had positive fare amounts but no passengers!"
        )

    # preparation of weather data
    humidity = prepare_weather_data_source(
        "humidity",
        expected_weather_data_schema,
        Config.weather_directory,
        Config.weather_file_ext,
        Config.regions,
        Config.common_timestamp_column,
        Config.null_threshold,
    )

    # humidity is measured in percent
    check_range(humidity, "humidity", 0, 100)

    pressure = prepare_weather_data_source(
        "pressure",
        expected_weather_data_schema,
        Config.weather_directory,
        Config.weather_file_ext,
        Config.regions,
        Config.common_timestamp_column,
        Config.null_threshold,
    )

    # pressure measured in hectopascal (hpa)
    # max recorded at sea level: 1084 hpa, min about 870 hpa, any values outside of this range are unusual
    check_range(pressure, "pressure", 870, 1084)

    temperature = prepare_weather_data_source(
        "temperature",
        expected_weather_data_schema,
        Config.weather_directory,
        Config.weather_file_ext,
        Config.regions,
        Config.common_timestamp_column,
        Config.null_threshold,
    )

    # temperature recorded in degrees kelvin
    # absolute zero is 0 while 330 is the highest temperature ever recorded on earth
    # so anything outside of this range should be considered unusual
    check_range(temperature, "temperature", 0, 330)

    wind_speed = prepare_weather_data_source(
        "wind_speed",
        expected_weather_data_schema,
        Config.weather_directory,
        Config.weather_file_ext,
        Config.regions,
        Config.common_timestamp_column,
        Config.null_threshold,
    )

    # Wind speed is measured in mph
    check_range(wind_speed, "wind_speed", 0, 100)

    # All weather data tables have the same timestamp column required for joining them
    weather_data = (
        humidity.join(pressure, on=Config.common_timestamp_column)
        .join(temperature, on=Config.common_timestamp_column)
        .join(wind_speed, on=Config.common_timestamp_column)
    )

    # Years, months, days and hours are extracted to make the data joinable with the taxi trips
    weather_data = extract_datetime_information(
        weather_data, Config.common_timestamp_column
    )

    # To reduce the size of the data that must be joined agains the taxi trips, the data
    # is filtered down to values set in config.py, which also correspond to the
    # single batch of taxi data
    weether_data = weather_data.filter(
        pl.col("year").is_in(Config.years) & pl.col("month").is_in(Config.months)
    )

    taxi_df = taxi_df.join(
        weather_data,
        how="left",
        left_on=["year", "month", "day", "hour"],
        right_on=["year", "month", "day", "hour"],
    )

    if not os.path.exists(Config.output_dir):
        os.mkdir(Config.output_dir)

    output_file = os.path.join(Config.output_dir, Config.output_file)
    print(f"Writing file {output_file}")
    taxi_df.write_parquet(output_file)

    print(f"Shape of final data: {taxi_df.shape}")
