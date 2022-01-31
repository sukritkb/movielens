import logging


from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit, col

from jobs.job import Job
from common.utils import CleanFunctions, Utils
from common.reader.csv import CSVReader
from common.writer.delta import DeltaWriter

logger = logging.getLogger(__name__)


class StageMoviesTags(Job):
    def compute(self):
        try:
            movies_path = Utils.remove_trailing_slash(self.jc.file_loc) + "/movies.csv"

            tags_path = Utils.remove_trailing_slash(self.jc.file_loc) + "/tags.csv"

            sink_path = Utils.remove_trailing_slash(self.jc.sink)

            reader = CSVReader(self.jc.spark)
            writer = DeltaWriter(self.jc.spark)

            movies_df = (
                reader.read(movies_path)
                .withColumn("run_date", lit(self.jc.run_date))
                .withColumn(
                    "movieId", CleanFunctions.clean_numeric(col("movieId"), "int")
                )
                .withColumn("title", CleanFunctions.clean_string(col("title")))
                .withColumn("genres", CleanFunctions.clean_string(col("genres")))
            )

            writer.write_to_table(
                movies_df,
                "default",
                "stage_movies",
                sink_path + "/stage_movies",
                ["run_date", "movieId"],
            )

            tags_df = (
                reader.read(tags_path)
                .withColumn("run_date", lit(self.jc.run_date))
                .withColumn(
                    "userId", CleanFunctions.clean_numeric(col("userId"), "int")
                )
                .withColumn(
                    "movieId", CleanFunctions.clean_numeric(col("movieId"), "int")
                )
                .withColumn("tags", CleanFunctions.clean_string(col("tags")))
                .withColumn(
                    "timestamp", CleanFunctions.clean_numeric(col("timestamp"), "float")
                )
            )

            writer.write_to_table(
                tags_df,
                "default",
                "stage_tags",
                sink_path + "/stage_tags",
                ["run_date", "movieId", "userId"],
            )

        except AnalysisException:
            logger.error(
                "Encountered error while running compute for %s", self.jc.job_name
            )
            raise
