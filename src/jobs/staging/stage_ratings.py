import logging

from delta.tables import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit

from jobs.job import Job
from jobs.context import JobContext
from common.utils import Utils
from common.reader.csv import CSVReader
from common.writer.delta import DeltaWriter

logger = logging.getLogger(__name__)


class StageRatings(Job):
    def __init__(self, jc: JobContext) -> None:
        super().__init__(jc)

    def compute(self):
        try:
            ratings_path = (
                Utils.remove_trailing_slash(self.jc.file_loc) + "/ratings.csv"
            )
            ratings_df = (
                CSVReader(self.jc.spark)
                .read(
                    ratings_path,
                    {"header": "true", "mergeSchema": "true", "inferSchema": "true"},
                )
                .withColumn("run_date", lit(self.jc.run_date))
            )
            column_set = {
                "userId": "userId",
                "movieId": "movieId",
                "rating": "rating",
                "timestamp": "timestamp",
            }

            writer = DeltaWriter(self.jc.spark)
            writer.upsert_to_table(
                ratings_df,
                self.jc.sink,
                ["run_date", "movieId", "userId"],
                column_set,
                column_set,
            )

        except AnalysisException:
            raise
