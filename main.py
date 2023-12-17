import polars as pl
from utils import (
    compare_schemas,
    prepare_weather_data_source,
    extract_datetime_information,
    print_df,
    check_for_nulls,
)
from metadata import expected_taxi_data_schema, expected_weather_data_schema
from config import Config

# TODO: implement data checks for taxi and weather data
# TODO: design diagram with draw.io
# TODO: add argparser to process inputs


if __name__ == "__main__":
    # preparation of taxi data
    taxi_df = pl.read_parquet(taxi_files)

    compare_schemas(expected_taxi_data_schema, taxi_df.schema)
    check_for_nulls(taxi_df, Config.null_threshold)

    taxi_df = extract_datetime_information(taxi_df, "tpep_pickup_datetime")

    # preparation of weather data
    humidity = prepare_weather_data_source(
        "humidity",
        Config.weather_directory,
        Config.weather_file_ext,
        Config.regions,
        Config.common_timestamp_column,
        Config.expected_weather_data_schema,
        Config.null_threshold,
    )

    pressure = prepare_weather_data_source(
        "pressure",
        Config.weather_directory,
        Config.weather_file_ext,
        Config.regions,
        Config.common_timestamp_column,
        Config.expected_weather_data_schema,
        Config.null_threshold,
    )

    temperature = prepare_weather_data_source(
        "temperature",
        Config.weather_directory,
        Config.weather_file_ext,
        Config.regions,
        Config.common_timestamp_column,
        Config.expected_weather_data_schema,
        Config.null_threshold,
    )

    wind_speed = prepare_weather_data_source(
        "wind_speed",
        Config.weather_directory,
        Config.weather_file_ext,
        Config.regions,
        Config.common_timestamp_column,
        Config.expected_weather_data_schema,
        Config.null_threshold,
    )

    # Joining weather data
    weather_data = (
        humidity.join(pressure, on=common_timestamp_column)
        .join(temperature, on=common_timestamp_column)
        .join(wind_speed, on=common_timestamp_column)
    )

    weather_data = extract_datetime_information(weather_data, common_timestamp_column)

    weether_data = weather_data.filter(
        pl.col("year").is_in(years) & pl.col("month").is_in(months)
    )

    taxi_df = taxi_df.join(
        weather_data,
        how="left",
        left_on=["year", "month", "day", "hour"],
        right_on=["year", "month", "day", "hour"],
    )

    print_df(taxi_df)
