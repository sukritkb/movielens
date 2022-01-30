from typing import Dict

from pyspark.sql import SparkSession, DataFrame

from common.constants import DataFormats
from common.writer.writer import Writer


class DeltaWriter(Writer):
    def __init__(self, sc: SparkSession) -> None:
        super().__init__(sc)

    def write(
        self,df: DataFrame, path: str, mode: str = None, options: Dict[str, str] = None
    ) -> DataFrame:
        return super().write(df,DataFormats.DELTA, path, mode, options)

    # def write_to_table()