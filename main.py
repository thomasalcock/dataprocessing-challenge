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
from config import Config

# TODO: design diagram with draw.io

if __name__ == "__main__":
    # preparation of taxi data
    taxi_df = pl.read_parquet(Config["taxi_files"])

    compare_schemas(expected_taxi_data_schema, taxi_df.schema)
    check_for_nulls(taxi_df, Config["null_threshold"])

    taxi_df = extract_datetime_information(taxi_df, "tpep_pickup_datetime")

    # preparation of weather data
    humidity = prepare_weather_data_source(
        "humidity",
        expected_weather_data_schema,
        Config["weather_directory"],
        Config["weather_file_ext"],
        Config["regions"],
        Config["common_timestamp_column"],
        Config["null_threshold"],
    )

    check_range(humidity, "humidity", 0, 100)

    pressure = prepare_weather_data_source(
        "pressure",
        expected_weather_data_schema,
        Config["weather_directory"],
        Config["weather_file_ext"],
        Config["regions"],
        Config["common_timestamp_column"],
        Config["null_threshold"],
    )

    # pressure measured in hectopascal (hpa)
    # max recorded at sea level: 1050 hpa, min about 850 hpa
    check_range(pressure, "pressure", 850, 1050)

    temperature = prepare_weather_data_source(
        "temperature",
        expected_weather_data_schema,
        Config["weather_directory"],
        Config["weather_file_ext"],
        Config["regions"],
        Config["common_timestamp_column"],
        Config["null_threshold"],
    )

    # temperature recorde in degrees fahrenheit
    wind_speed = prepare_weather_data_source(
        "wind_speed",
        expected_weather_data_schema,
        Config["weather_directory"],
        Config["weather_file_ext"],
        Config["regions"],
        Config["common_timestamp_column"],
        Config["null_threshold"],
    )

    # Joining weather data
    weather_data = (
        humidity.join(pressure, on=Config["common_timestamp_column"])
        .join(temperature, on=Config["common_timestamp_column"])
        .join(wind_speed, on=Config["common_timestamp_column"])
    )

    weather_data = extract_datetime_information(
        weather_data, Config["common_timestamp_column"]
    )

    weether_data = weather_data.filter(
        pl.col("year").is_in(Config["years"]) & pl.col("month").is_in(Config["months"])
    )

    taxi_df = taxi_df.join(
        weather_data,
        how="left",
        left_on=["year", "month", "day", "hour"],
        right_on=["year", "month", "day", "hour"],
    )

    if not os.path.exists(Config["output_dir"]):
        os.mkdir(Config["output_dir"])

    output_file = os.path.join(Config["output_dir"], Config["output_file"])
    print(f"Writing file {output_file}")
    taxi_df.write_parquet(output_file)

    print(f"Shape of final data: {taxi_df.shape}")
