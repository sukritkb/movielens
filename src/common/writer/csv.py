from typing import Dict

from pyspark.sql import SparkSession, DataFrame

from common.constants import DataFormats
from common.writer.writer import Writer


class CSVWriter(Writer):
    def __init__(self, sc: SparkSession) -> None:
        super().__init__(sc)
        self.sc = sc

    def write(
        self, df: DataFrame, path: str, mode: str = None, options: Dict[str, str] = None
    ):
        super().write(df, DataFormats.CSV, path, mode, options)

