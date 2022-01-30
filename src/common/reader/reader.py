from typing import Dict
import logging


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

from common.constants import DataFormats,DEFAULT_READ_OPTIONS

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
            logger.info(f"Trying to read {format} dataframe from {path}")
            options = [options, DEFAULT_READ_OPTIONS][options is None]
            df = self.spark.read.format(format.value).options(**options).load(path)
            logger.info(f"Successfully read dataframe from {path}")
            return df
        except AnalysisException:
            logger.error(f"Path does not exist: {path} ")
            raise
        except Exception:
            raise

    # def read_from_table(self,query:str=None,db_name:str=None,table_name:str=None) -> DataFrame:
    #     """
    #     Method to read dataframes by passing a query or database name and table
        
    #     If query is not passed then select * from db_name.table is returned

    #     Parameters
    #     ----------

    #     query: str, 
    #         SQL query to be run to return the dataframe
    #     db_name: str, optional
    #         Database name for the table
    #     table_name: str, optional
    #         Table name to fetch data 

    #     Returns
    #     -------
    #     df: DataFrame, 
    #         a spark dataframe object 
        
    #     Raises
    #     ------
    #     AnalysisException:
    #         if table or view is not found 

    #     """
    #     try:
    #         logger.info(f"Trying to read {format} dataframe from {path}")
    #         options = [options, self.default_options][options is None]
    #         df = self.spark.read.format(format.value).options(**options).load(path)
    #         logger.info(f"Successfully read dataframe from {path}")
    #         return df
    #     except AnalysisException:
    #         logger.error(f"Path does not exist: {path} ")
    #         raise
    #     except Exception:
    #         raise