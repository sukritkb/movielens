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
            movies_path = Utils.remove_trailing_slash(
                self.jc.file_loc) + "/movies.csv"

            sink_path = Utils.remove_trailing_slash(
                self.jc.sink)+"/stage_movies"

            reader = CSVReader(self.jc.spark)
            writer = DeltaWriter(self.jc.spark)

            movies_df = (
                reader.read(movies_path)
                .withColumn("run_date", lit(self.jc.run_date))
                .withColumn(
                    "movieId", CleanFunctions.clean_numeric(
                        col("movieId"), "int")
                )
                .withColumn("title", CleanFunctions.clean_string(col("title")))
                .withColumn("genres", CleanFunctions.clean_string(col("genres")))
            )

            writer.write_to_table(
                movies_df,
                "default",
                "stage_movies",
                sink_path,
                ["run_date", "movieId"],
            )

        except AnalysisException:
            logger.error(
                "Encountered error while running compute for %s", self.jc.job_name
            )
            raise
