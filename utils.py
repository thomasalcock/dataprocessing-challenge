import polars as pl
import polars.selectors as cs
from polars.exceptions import ColumnNotFoundError
from typing import List, OrderedDict, Union


def prepare_weather_data_source(
    source: str,
    expected_schema: OrderedDict,
    directory: str,
    file_ext: str,
    regions: List[str],
    timestamp_column: str,
    null_threshold: float,
) -> pl.DataFrame:
    """
    Handles all data preparation steps / data checks for a single weather data source.
    Weather data measurements are stored in separate csv files (pressure, humidity, etc.)
    and each source should be processed / checked separately before being merged with other
    data sources.
    """

    df = pl.read_csv(f"{directory}/{source}.{file_ext}")
    compare_schemas(expected_schema, df.schema)
    df = df.select(timestamp_column, cs.by_name(regions))
    for region in regions:
        df = df.rename({f"{region}": f"{source}"})
    df = try_datetime_conversion(df, timestamp_column)
    check_for_nulls(df, null_threshold)
    return df


def print_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Prints a polars dataframe with all columns visible. Default printing behaviour replaces
    columns with '...', hence this function.
    """

    with pl.Config(tbl_cols=df.width):
        print(df)


def try_datetime_conversion(
    df: pl.DataFrame, column: str, format: str = "%Y-%m-%d %H:%M:%S"
):
    """
    Attempts to parse a given column to a timestamp using the given format. Throws a specific
    error if the column values cannot be parsed. The 'strict' argument ensures that polars
    will raise an error.
    """
    try:
        df = df.with_columns(pl.col(column).str.to_datetime(strict=True, format=format))
        return df
    except Exception as e:
        raise ValueError(f"column {column} cannot be parsed to format: {format}: {e}")


def extract_datetime_information(df: pl.DataFrame, column: str):
    """
    Extracts the year, month, day and hour from a timestamp column.
    Several tables require the same information be extracted, hence this function.

    :param df: a polars dataframe
    :return
    """
    return df.with_columns(
        pl.col(column).dt.year().alias("year"),
        pl.col(column).dt.month().alias("month"),
        pl.col(column).dt.day().alias("day"),
        pl.col(column).dt.hour().alias("hour"),
    )


def check_for_nulls(df: pl.DataFrame, threshold: float):
    null_count = df.null_count().to_dict().items()
    row_count = df.shape[0]
    for key, value in null_count:
        null_ratio = round(100 * value[0] / row_count, 2)
        if null_ratio >= threshold:
            print(f"{key} has {null_ratio}% of null values")


def compare_schemas(expected: OrderedDict, actual: OrderedDict):
    expected_len = len(expected)
    actual_len = len(actual)
    if expected_len != actual_len:
        print("Schemas must have same number of elements!")
    for actual_value, expected_value in zip(actual.items(), expected.items()):
        if actual_value != expected_value:
            print(
                f"Field: {actual_value} does not match expected field: {expected_value}"
            )


def check_range(
    df: pl.DataFrame,
    column: str,
    min_value: Union[int, float],
    max_value: Union[int, float],
):
    out_of_range = df.filter(
        (pl.col(column) > max_value) | (pl.col(column) < min_value)
    )
    n_out_of_range = out_of_range.shape[0]
    if n_out_of_range > 0:
        print(
            f"{n_out_of_range} rows of {column} either below {min_value} or above {max_value}!",
        )
        print_df(out_of_range)
