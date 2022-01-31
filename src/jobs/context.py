import importlib
import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class JobContext:
    """
    Class to instantiate a job context to be passed to jobs 

    """

    def __init__(
        self,
        sc: SparkSession,
        job_name: str,
        class_name: str,
        file_loc: str,
        sink_loc: str,
        run_date: str,
    ) -> None:
        self.spark = sc
        self.job_name = job_name
        self.class_name = class_name
        self.file_loc = file_loc
        self.sink = sink_loc
        self.run_date = run_date

    def run_job(self):
        try:
            logger.info("Trying to import %s module", self.job_name)
            job_module = importlib.import_module(f"jobs.{self.job_name}")
            getattr(job_module, self.class_name)(self).compute()
        except ModuleNotFoundError:
            logger.error("No module named %s was found", self.job_name)
            raise
        except AttributeError:
            logger.error("Class name: %s not found.", self.class_name)
            raise
