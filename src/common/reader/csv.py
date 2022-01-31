from typing import Dict

from pyspark.sql import SparkSession, DataFrame

from common.reader.reader import Reader
from common.constants import DEFAULT_CSV_READ_OPTIONS, DataFormats


class CSVReader(Reader):
    def __init__(self, sc: SparkSession) -> None:
        super().__init__(sc)
        self.sc = sc

    def read(
        self, path: str, options: Dict[str, str] = DEFAULT_CSV_READ_OPTIONS
    ) -> DataFrame:
        super().read(DataFormats.CSV, path, options)
