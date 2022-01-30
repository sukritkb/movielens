from itertools import count
import logging

from delta.tables import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import *
from pyspark.sql.window import Window, WindowSpec

from jobs.job import Job
from jobs.context import JobContext
from common.utils import CleanFunctions, Utils
from common.reader.csv import CSVReader
from common.writer.csv import CSVWriter

logger = logging.getLogger(__name__)


class TransformTopTen(Job):
    def __init__(self, jc: JobContext) -> None:
        super().__init__(jc)

    def compute(self):
        ratings_path = Utils.remove_trailing_slash(self.jc.file_loc) + "/ratings.csv"
        sink_path = Utils.remove_trailing_slash(self.jc.sink) + "/top_ten"
        ratings_df = (
            CSVReader(self.jc.spark)
            .read(ratings_path)
            .withColumn("run_date", lit(self.jc.run_date))
        )

        window_spec = Window.partitionBy("movieId")

        result_df = ratings_df.withColumn("cnt_ratings", sum(lit(1)).over(window_spec)).withColumn(
            "avg_ratings", avg(col("rating")).over(window_spec)
        ).filter("cnt_ratings>10").orderBy(desc("avg_ratings")).select(
            "movieId"
        ).distinct().limit(
            10
        ).coalesce(1)

        CSVWriter(self.jc.spark).write(result_df,sink_path)




        

