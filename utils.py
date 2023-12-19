import polars as pl
import polars.selectors as cs
from polars.exceptions import ColumnNotFoundError


def prepare_weather_data_source(
    source,
    expected_schema,
    directory,
    file_ext,
    regions,
    timestamp_column,
    null_threshold,
):
    df = pl.read_csv(f"{directory}/{source}.{file_ext}")
    compare_schemas(expected_schema, df.schema)
    df = df.select(timestamp_column, cs.by_name(regions))
    for region in regions:
        df = df.rename({f"{region}": f"{source}"})
    df = try_datetime_conversion(df, timestamp_column)
    check_for_nulls(df, null_threshold)
    return df


def print_df(df):
    with pl.Config(tbl_cols=df.width):
        print(df)


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


def check_for_nulls(df, threshold):
    null_count = df.null_count().to_dict().items()
    row_count = df.shape[0]
    for key, value in null_count:
        null_ratio = round(100 * value[0] / row_count, 2)
        if null_ratio >= threshold:
            print(f"{key} has {null_ratio}% of null values")


def compare_schemas(expected, actual):
    expected_len = len(expected)
    actual_len = len(actual)
    if expected_len != actual_len:
        print("Schemas must have same number of elements!")
    for actual_value, expected_value in zip(actual.items(), expected.items()):
        if actual_value != expected_value:
            print(
                f"Field: {actual_value} does not match expected field: {expected_value}"
            )


def check_range(df, column, min_value, max_value):
    n_out_of_range = df.filter(
        (pl.col(column) > max_value) | (pl.col(column) < min_value)
    ).shape[0]
    if n_out_of_range > 0:
        print(
            f"{n_out_of_range} rows of {column} either below {min_value} or above {max_value}!"
        )
