import argparse
import enum
import importlib
import time
import os
import sys
import logging.config
import logging
import json

from pyspark.sql import SparkSession
from pyspark import SparkConf

from common.constants import DEFAULT_SPARK_CONF, DataFormats, JobType
from common.reader.reader import Reader
from jobs.context import JobContext

# if os.path.exists("libs.zip"):
#     sys.path.insert(0, "libs.zip")
# else:
#     sys.path.insert(0, "./libs")

# if os.path.exists("jobs.zip"):
#     sys.path.insert(0, "jobs.zip")
# else:
#     sys.path.insert(0, "./jobs")

# pylint:disable=E0401
try:
    import pyspark
except:
    import findspark

    findspark.init()
    import pyspark

__author__ = "sukritkb"


if __name__ == "__main__":

    print(os.path)
    with open("src/logging.json") as log_json:
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
    logger.info(f"Called with arguments: {args}")

    environment = {
        "PYSPARK_JOB_ARGS": " ".join(args.spark_args) if args.spark_args else ""
    }

    conf = SparkConf()
    if args.spark_args:
        spark_args_tuples = [arg_str.split("=") for arg_str in args.spark_args]
        logger.info(f"spark_args_tuples: {spark_args_tuples}")
        conf.setAll(spark_args_tuples)
    else:
        conf.setAll(DEFAULT_SPARK_CONF)

    logger.info(f"\nRunning job {args.job_name}...\nenvironment is {environment}\n")

    os.environ.update(environment)
    sc = SparkSession.builder.appName(args.job_name).config(conf=conf).getOrCreate()

    start = time.time()
    jc = JobContext(sc, args.job_name, args.class_name, args.file_loc, args.sink_loc)
    jc.run_job()
    end = time.time()

    logger.info(f"\nExecution of job {args.job_name} took {end-start} seconds")
