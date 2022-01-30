from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame

from common.constants import DataFormats, DEFAULT_CSV_WRITE_OPTIONS
from common.writer.writer import Writer


class CSVWriter(Writer):
    def __init__(self, sc: SparkSession) -> None:
        super().__init__(sc)
        self.sc = sc

    def write(
        self,
        df: DataFrame,
        path: str,
        partition_columns: List[str] = None,
        mode: str = None,
        options: Dict[str, str] = DEFAULT_CSV_WRITE_OPTIONS,
    ):
        super().write(df, DataFormats.CSV, path, partition_columns, mode, options)

