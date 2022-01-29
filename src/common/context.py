from pyspark.sql import SparkSession
import os 

class JobContext:
    def __init__(self,sc: SparkSession) -> None:
        self.spark = sc

    def run_job(self,job_name: str,type: str) -> None:
        pass