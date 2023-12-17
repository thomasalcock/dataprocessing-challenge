import polars as pl
from utils import *

months = [1]
years = [2016]
region = "New York"
common_timestamp_column = "datetime"
weather_directory = "weather_data"
weather_file_ext = "csv"
taxi_files = "taxi_data/*parquet"

# TODO: implement schema comparison for taxi data, each weather data source, issue warnings if schemas dont match
# TODO: implement data checks for

if __name__ == "__main__":
    schema = get_taxi_schema()
    taxi_df = pl.read_parquet(taxi_files)

    taxi_df = extract_datetime_information(taxi_df, "tpep_pickup_datetime")

    humidity = prepare_weather_data_source(
        "humidity", weather_directory, weather_file_ext, region, common_timestamp_column
    )
    pressure = prepare_weather_data_source(
        "pressure", weather_directory, weather_file_ext, region, common_timestamp_column
    )

    temperature = prepare_weather_data_source(
        "temperature",
        weather_directory,
        weather_file_ext,
        region,
        common_timestamp_column,
    )

    wind_speed = prepare_weather_data_source(
        "wind_speed",
        weather_directory,
        weather_file_ext,
        region,
        common_timestamp_column,
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
