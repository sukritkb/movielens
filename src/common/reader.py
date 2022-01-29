from typing import Dict
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

from constants import DataFormats

logger = logging.getLogger(__name__)


class Reader:
    """
    Class used for handling spark reads for jobs

    """

    def __init__(self, sc: SparkSession) -> None:
        self.spark = sc
        self.default_options = {"mergeSchema": "true"}

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
            options = [options, self.default_options][options is None]
            df = self.spark.read.format(format.value).options(options)
        except AnalysisException:
            logging.error(f"Path does not exist: {path} ")
            raise
        except Exception:
            raise
        finally:
            return df

