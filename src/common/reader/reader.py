import logging
from typing import Dict

from common.constants import DEFAULT_READ_OPTIONS, DataFormats
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class Reader:
    """
    Class used for handling spark reads for jobs

    """

    def __init__(self, sc: SparkSession) -> None:
        self.spark = sc

    def read(
        self, format: DataFormats, path: str, options: Dict[str, str] = None,
    ) -> DataFrame:
        """
        Method to read dataframes by passing format, path and options

        If the argument `options` is not passed default options are used 

        Parameters
        ----------

        format: DataFormat, 
            The file format for the dataframe to be read 
        path: str,
            Path of the dataframe to be read
        options: Dict[str,str], optional
            Read options for the underlying datasource

        Returns
        -------
        df: DataFrame, 
            a spark dataframe object

        Raises
        ------
        AnalysisException:
            if path which we are trying to read is not found

        """
        try:
            logger.info("Trying to read %s dataframe from %s", format, path)
            options = [options, DEFAULT_READ_OPTIONS][options is None]
            df = self.spark.read.format(
                format.value).options(**options).load(path)
            logger.info("Successfully read dataframe from %s", path)
            return df
        except AnalysisException:
            logger.error("Path does not exist: %s ", path)
            raise
