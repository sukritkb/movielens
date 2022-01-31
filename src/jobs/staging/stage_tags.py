import logging


from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit, col

from jobs.job import Job
from common.utils import CleanFunctions, Utils
from common.reader.csv import CSVReader
from common.writer.delta import DeltaWriter

logger = logging.getLogger(__name__)


class StageTags(Job):
    def compute(self):
        try:

            tags_path = Utils.remove_trailing_slash(
                self.jc.file_loc) + "/tags.csv"

            sink_path = Utils.remove_trailing_slash(
                self.jc.sink) + "/stage_tags"

            reader = CSVReader(self.jc.spark)
            writer = DeltaWriter(self.jc.spark)

            tags_df = (
                reader.read(tags_path)
                .withColumn("run_date", lit(self.jc.run_date))
                .withColumn(
                    "userId", CleanFunctions.clean_numeric(
                        col("userId"), "int")
                )
                .withColumn(
                    "movieId", CleanFunctions.clean_numeric(
                        col("movieId"), "int")
                )
                .withColumn("tag", CleanFunctions.clean_string(col("tag")))
                .withColumn(
                    "timestamp", col("timestamp").cast("timestamp")
                )
            )

            writer.write_to_table(
                tags_df,
                "default",
                "stage_tags",
                sink_path,
                ["run_date", "movieId", "userId"],
            )

        except AnalysisException:
            logger.error(
                "Encountered error while running compute for %s", self.jc.job_name
            )
            raise
