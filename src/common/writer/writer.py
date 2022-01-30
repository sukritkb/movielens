from typing import Dict, List
import logging


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError


from common.constants import DataFormats, DEFAULT_WRITE_OPTIONS

logger = logging.getLogger(__name__)


class Writer:
    """
    Class used for handling spark writes for jobs

    """

    def __init__(self, sc: SparkSession) -> None:
        self.spark = sc

    def write(
        self,
        df: DataFrame,
        format: DataFormats,
        path: str,
        partition_columns: List[str] = None,
        mode: str = None,
        options: Dict[str, str] = None,
    ):
        """
        Method to write dataframes by passing format, path and options
        
        If the argument `options` is not passed default write options are used 
        If the argument `mode` is not passed then default mode overwrite is used

        Parameters
        ----------

        df: DataFrame, 
            Dataframe which needs to be saved
        format: DataFormat, 
            The file format for the dataframe to be written 
        path: str,
            Path of the dataframe to be written
        partition_columns: List[str], optional
            List of partition columns
        mode: str, optional
            Save mode to be used
        options: Dict[str,str], optional
            Write options for the underlying datasource

        Returns
        -------
        df: DataFrame, 
            a spark dataframe object 
        
        Raises
        ------
        IOException:
            if unable to create the directory in the specified path     

        """
        try:
            logger.info(f"Trying to write {format} dataframe to {path}")
            options = [options, DEFAULT_WRITE_OPTIONS][options is None]
            mode = [mode, "overwrite"][mode is None]

            if not partition_columns:
                df.write.format(format.value).mode(mode).options(**options).save(path)
            else:
                df.write.format(format.value).mode(mode).options(**options).partitionBy(
                    *partition_columns
                ).save(path)
            logger.info(f"Successfully saved dataframe to {path}")

        except IOError:
            logger.error(f"Unable to create directory: {path}")
            raise
        except Exception:
            raise
