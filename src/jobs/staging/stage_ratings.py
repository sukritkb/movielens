import logging


from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit, col

from jobs.job import Job
from common.utils import Utils
from common.reader.csv import CSVReader
from common.writer.delta import DeltaWriter
from common.utils import CleanFunctions

logger = logging.getLogger(__name__)


class StageRatings(Job):
    def compute(self):
        try:
            ratings_path = (
                Utils.remove_trailing_slash(self.jc.file_loc) + "/ratings.csv"
            )
            ratings_df = (
                CSVReader(self.jc.spark)
                .read(ratings_path)
                .withColumn("run_date", lit(self.jc.run_date))
                .withColumn("userId", CleanFunctions.clean_string(col("userId")))
                .withColumn("movieId", CleanFunctions.clean_string(col("movieId")))
                .withColumn("rating", CleanFunctions.clean_numeric(col("rating"), "float"))
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
                "default",
                "ratings",
                ["run_date", "movieId"],
            )

        except AnalysisException:
            logger.error(
                "Encountered an error while running compute for %s", self.jc.job_name
            )
            raise
