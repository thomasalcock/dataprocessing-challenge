import polars as pl
from utils import (
    compare_schemas,
    prepare_weather_data_source,
    extract_datetime_information,
    print_df,
)
from metadata import expected_taxi_data_schema, expected_weather_data_schema

months = [1]
years = [2016]
regions = ["New York"]
common_timestamp_column = "datetime"
weather_directory = "weather_data"
weather_file_ext = "csv"
taxi_files = "taxi_data/*parquet"

# TODO: implement data checks for taxi and weather data
# TODO: design diagram with draw.io

# TODO: add argparser to process inputs


if __name__ == "__main__":
    taxi_df = pl.read_parquet(taxi_files)

    compare_schemas(expected_taxi_data_schema, taxi_df.schema)

    taxi_df = extract_datetime_information(taxi_df, "tpep_pickup_datetime")

    humidity = prepare_weather_data_source(
        "humidity",
        weather_directory,
        weather_file_ext,
        regions,
        common_timestamp_column,
        expected_weather_data_schema,
    )

    pressure = prepare_weather_data_source(
        "pressure",
        weather_directory,
        weather_file_ext,
        regions,
        common_timestamp_column,
        expected_weather_data_schema,
    )

    temperature = prepare_weather_data_source(
        "temperature",
        weather_directory,
        weather_file_ext,
        regions,
        common_timestamp_column,
        expected_weather_data_schema,
    )

    wind_speed = prepare_weather_data_source(
        "wind_speed",
        weather_directory,
        weather_file_ext,
        regions,
        common_timestamp_column,
        expected_weather_data_schema,
    )

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
