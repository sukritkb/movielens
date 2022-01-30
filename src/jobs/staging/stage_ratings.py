import logging

from delta.tables import *
from pyspark.sql.utils import AnalysisException

from jobs.job import Jobs
from jobs.context import JobContext
from common.utils import Utils
from common.reader.csv import CSVReader

logger = logging.getLogger(__name__)


class StageRatings(Jobs):
    def __init__(self, jc: JobContext) -> None:
        super().__init__(jc)

    def compute(self):
        try:
            ratings_path = (
                Utils.remove_trailing_slash(self.jc.file_loc) + "/ratings.csv"
            )
            ratings_df = CSVReader(self.jc.spark).read(
                ratings_path,
                {"header": "true", "mergeSchema": "true", "inferSchema": "true"},
            )
            ratings_df.show()
            ratings_df.printSchema()
        except AnalysisException:
            logger.error(f"Path not found {ratings_path} for ratings df")

