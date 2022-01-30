from typing import List, Dict
import logging

from pyspark.sql import Column
from pyspark.sql.functions import *

logger = logging.getLogger(__name__)


class Utils:
    """
    Common functions used throughout the modules 

    """

    @staticmethod
    def remove_trailing_slash(path: str):
        return path.rstrip("/")

    @staticmethod
    def get_join_conditions(join_columns: List[str]) -> str:
        return " and ".join(
            list(map(lambda column: f"target.{column}=updates.{column}", join_columns))
        )

    @staticmethod
    def modify_upsert_set(upsert_set: Dict[str, str]) -> Dict[str, str]:
        return dict(map(lambda kv: (kv[0], f"updates.{kv[1]}"), upsert_set.items()))


class CleanFunctions:
    """
    Clean functions for safe typecasting and null filling 
    """

    @staticmethod
    def clean_string(
        column: Column, invalid_values: List[str] = ["", "null", " "]
    ) -> Column:
        return when(~column.isin(invalid_values), column).otherwise(
            lit(None).cast("string")
        )

    @staticmethod
    def clean_numeric(column: Column, data_type: str) -> Column:
        return when(column.cast(data_type).isNotNull(), column).otherwise(
            lit(None).cast(data_type)
        )

