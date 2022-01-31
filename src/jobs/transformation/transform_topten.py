import logging


from pyspark.sql.functions import sum, col, avg, lit, desc
from pyspark.sql.window import Window

from jobs.job import Job
from common.utils import Utils
from common.reader.csv import CSVReader
from common.writer.csv import CSVWriter

logger = logging.getLogger(__name__)


class TransformTopTen(Job):
    def compute(self):
        ratings_path = Utils.remove_trailing_slash(self.jc.file_loc) + "/ratings.csv"
        sink_path = Utils.remove_trailing_slash(self.jc.sink) + "/top_ten"
        ratings_df = (
            CSVReader(self.jc.spark)
            .read(ratings_path)
            .withColumn("run_date", lit(self.jc.run_date))
        )

        window_spec = Window.partitionBy("movieId")

        result_df = (
            ratings_df.withColumn("cnt_ratings", sum(lit(1)).over(window_spec))
            .withColumn("avg_rating", avg(col("rating")).over(window_spec))
            .select("movieId", "cnt_ratings", "avg_rating")
            .filter("cnt_ratings>10")
            .distinct()
            .orderBy(desc("avg_rating"))
            .select("movieId")
            .coalesce(1)
        )

        CSVWriter(self.jc.spark).write(result_df, sink_path)
