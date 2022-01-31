import argparse
import importlib.resources
import time
import os
import logging.config
import logging
import json
from datetime import date


from pyspark.sql import SparkSession
from pyspark import SparkConf

from common.constants import DEFAULT_SPARK_CONF
from jobs.context import JobContext


try:
    import pyspark
except BaseException:
    import findspark

    findspark.init()
    import pyspark

__author__ = "sukritkb"


if __name__ == "__main__":

    with importlib.resources.open_text("configs", "logging.json") as log_json:
        logging_config = json.load(log_json)
    logging.config.dictConfig(logging_config)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Run a PySpark job")
    parser.add_argument(
        "--job-name",
        type=str,
        required=True,
        dest="job_name",
        help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)",
    )
    parser.add_argument(
        "--class-name",
        type=str,
        required=True,
        dest="class_name",
        help="The name of the class which contains the compute method",
    )
    parser.add_argument(
        "--file-loc",
        type=str,
        required=True,
        dest="file_loc",
        help="The location of resource files (csvs)",
    )
    parser.add_argument(
        "--sink-loc",
        type=str,
        required=True,
        dest="sink_loc",
        help="The location where you want to save the output",
    )
    parser.add_argument(
        "--spark-args",
        nargs="*",
        help="Spark arguments to send to the PySpark job (example: --spark-args spark.driver.memory=9g spark.master=local",
    )

    args = parser.parse_args()
    logger.info("Called with arguments: %s", args)

    environment = {
        "PYSPARK_JOB_ARGS": " ".join(args.spark_args) if args.spark_args else ""
    }

    run_date = date.today().strftime("%Y-%m-%d")

    conf = SparkConf()
    if args.spark_args:
        spark_args_tuples = [arg_str.split("=") for arg_str in args.spark_args]
        logger.info("spark_args_tuples: %s", spark_args_tuples)
        conf.setAll(spark_args_tuples)
    else:
        conf.setAll(DEFAULT_SPARK_CONF)

    logger.info("\nRunning job %s...\nenvironment is %s\n", args.job_name, environment)

    os.environ.update(environment)
    sc = SparkSession.builder.appName(args.job_name).config(conf=conf).getOrCreate()

    start = time.time()
    jc = JobContext(
        sc=sc,
        job_name=args.job_name,
        class_name=args.class_name,
        file_loc=args.file_loc,
        sink_loc=args.sink_loc,
        run_date=run_date,
    )
    jc.run_job()
    end = time.time()

    logger.info("\nExecution of job %s took %s seconds", args.job_name, end - start)
