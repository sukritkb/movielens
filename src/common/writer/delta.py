from typing import Dict
import logging

from pyspark.sql import SparkSession, DataFrame
from delta.tables import *
from pyspark.sql.utils import AnalysisException

from common.constants import DataFormats
from common.utils import Utils
from common.writer.writer import Writer

logger = logging.getLogger(__name__)

# TODO clear out analysis exceptions
# TODO pass spark session


class DeltaWriter(Writer):
    def __init__(self, sc: SparkSession) -> None:
        super().__init__(sc)
        self.sc = sc

    def write(
        self,
        df: DataFrame,
        path: str,
        partition_columns: List[str] = None,
        mode: str = None,
        options: Dict[str, str] = None,
    ):
        super().write(df, DataFormats.DELTA, path, partition_columns, mode, options)

    def write_to_table(
        self,
        df: DataFrame,
        db_name: str,
        table_name: str,
        path: str,
        partition_columns: List[str] = None,
        mode: str = None,
        options: Dict[str, str] = None,
    ):
        self.write(df, path, partition_columns, mode, options)
        self.sc.sql(
            f"CREATE TABLE if not exists {db_name}.{table_name} USING DELTA LOCATION '{path}'"
        )

    def upsert_to_table(
        self,
        dfUpdates: DataFrame,
        target_table: str,
        join_columns: List[str],
        when_matched_set: Dict[str, str],
        when_not_matched_set: Dict[str, str],
    ):
        """
        Delta table function to perform upserts

        Parameters
        ----------
        sc: SparkSession,
        df: DataFrame, 
            Spark dataframe which contains the updates 
        target_table: str,
            Path of the target table where upserts are to be done 
        join_columns: str, 
            Columns used to determine a match 
        when_matched_set: Dict[str,str],
            Dictionary to update columns when a match is found 
        when_not_matched_set: Dict[str,str],
            Dictionary to insert columns when a match is not found 

        """
        try:
            deltaTargetTable = DeltaTable.forPath(self.sc, target_table)
            join_condition = Utils.get_join_conditions(join_columns)
            when_matched_set = Utils.modify_upsert_set(when_matched_set)
            when_not_matched_set = Utils.modify_upsert_set(when_not_matched_set)
            deltaTargetTable.alias("target").merge(
                dfUpdates.alias("updates"), join_condition
            ).whenMatchedUpdate(set=when_matched_set).whenNotMatchedInsert(
                values=when_not_matched_set
            ).execute()

        except AnalysisException:
            raise

