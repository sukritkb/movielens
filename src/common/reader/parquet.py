from typing import Dict

from pyspark.sql import SparkSession, DataFrame

from common.reader.reader import Reader
from common.constants import DataFormats


class ParquetReader(Reader):
    def __init__(self, sc: SparkSession) -> None:
        super().__init__(sc)
        self.sc = sc

    def read(self, path: str, options: Dict[str, str] = None) -> DataFrame:
        super().read(DataFormats.PARQUET, path, options)
