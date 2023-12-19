from dataclasses import dataclass, field
from typing import List


@dataclass
class ConfigClass:
    months: List[int] = field(default_factory=lambda: [1])
    years: List[int] = field(default_factory=lambda: [2016])
    regions: List[str] = field(default_factory=lambda: ["New York"])
    common_timestamp_column: str = "datetime"
    weather_directory: str = "weather_data"
    weather_file_ext: str = "csv"
    taxi_files: str = "taxi_data/*parquet"
    null_threshold: float = 0.1
    output_dir: str = "output"
    output_file: str = "taxi_data_final.parquet"
