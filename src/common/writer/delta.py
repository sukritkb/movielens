import logging
from typing import Dict, List

from common.constants import DataFormats
from common.utils import Utils
from common.writer.writer import Writer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


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
        target_table_path: str,
        join_columns: List[str],
        when_matched_set: Dict[str, str],
        when_not_matched_set: Dict[str, str],
        db_name: str = None,
        target_table_name: str = None,
        partition_columns: List[str] = None,
        mode: str = None,
        options: Dict[str, str] = None,
    ):
        """
        Delta table function to perform upserts
        In case the target table is not present a new table is created. 

        Parameters
        ----------
        sc: SparkSession,
        df: DataFrame,
            Spark dataframe which contains the updates
        target_table_path: str,
            Path of the target table where upserts are to be done
        join_columns: str,
            Columns used to determine a match
        when_matched_set: Dict[str,str],
            Dictionary to update columns when a match is found
        when_not_matched_set: Dict[str,str],
            Dictionary to insert columns when a match is not found
        db_name: str, optional
            name of the database to be used - used to create the table if it doesnt exist
        target_table_name: str, optional 
            name of the table where upsert is required - used to create the table if it doesnt exist
        partition_columns: List[str], optional
            List of partition columns
        mode: str, optional
            Save mode to be used
        options: Dict[str,str], optional
            Write options for the underlying datasource
        """
        try:

            try:
                deltaTargetTable = DeltaTable.forPath(
                    self.sc, target_table_path)
            except AnalysisException:
                if db_name and target_table_name:
                    self.write_to_table(
                        dfUpdates,
                        db_name,
                        target_table_name,
                        target_table_path,
                        partition_columns,
                        mode,
                        options
                    )
                else:
                    logger.error(
                        "Target table not found and db name and table name were also not provided"
                    )
                    raise

            join_condition = Utils.get_join_conditions(join_columns)
            when_matched_set = Utils.modify_upsert_set(when_matched_set)
            when_not_matched_set = Utils.modify_upsert_set(
                when_not_matched_set)
            deltaTargetTable.alias("target").merge(
                dfUpdates.alias("updates"), join_condition
            ).whenMatchedUpdate(set=when_matched_set).whenNotMatchedInsert(
                values=when_not_matched_set
            ).execute()

        except AnalysisException:
            logger.error("Error while upserting to delta table.")
            raise
