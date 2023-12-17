import polars as pl
from polars.exceptions import ColumnNotFoundError
from collections import OrderedDict
from polars import Int64, Float64, Utf8, Datetime


def get_taxi_schema():
    OrderedDict(
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


def filter_data_by_years_months(df, years, months):
    df = df.filter(pl.col("year").is_in(years) & pl.col("month").is_in(months))
    return df


def prepare_weather_data_source(
    source,
    directory,
    file_ext,
    region,
    timestamp_column,
):
    df = pl.read_csv(f"{directory}/{source}.{file_ext}")
    check_if_column_exists(df, timestamp_column)
    df = df.select(timestamp_column, region).rename({f"{region}": f"{source}"})
    df = try_datetime_conversion(df, timestamp_column)
    return df


def print_df(df):
    with pl.Config(tbl_cols=df.width):
        print(df)


def check_if_column_exists(df, column):
    if not column in df.columns:
        raise ColumnNotFoundError(f"column {column} cannot be found!")


def try_datetime_conversion(df, column, format="%Y-%m-%d %H:%M:%S"):
    try:
        df = df.with_columns(pl.col(column).str.to_datetime(strict=True, format=format))
        return df
    except Exception as e:
        raise ValueError(f"column {column} cannot be parsed to format: {format}: {e}")


def extract_datetime_information(df, column):
    return df.with_columns(
        pl.col(column).dt.year().alias("year"),
        pl.col(column).dt.month().alias("month"),
        pl.col(column).dt.day().alias("day"),
        pl.col(column).dt.hour().alias("hour"),
    )
