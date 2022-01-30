from typing import List, Dict
import logging

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

