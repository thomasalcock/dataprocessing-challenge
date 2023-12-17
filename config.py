# Config dict
@dataclass
class Config:
    months: List[int] = [1]
    years: List[int] = [2016]
    regions: List[str] = ["New York"]
    common_timestamp_column: str = "datetime"
    weather_directory: str = "weather_data"
    weather_file_ext: str = "csv"
    taxi_files: str = "taxi_data/*parquet"
    null_threshold: float = 0.1
