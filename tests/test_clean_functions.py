import pytest
from src.common.utils import CleanFunctions
from pyspark.sql.functions import col
from chispa import assert_column_equality


@pytest.mark.usefixtures("spark_session")
def test_clean_numeric(spark_session):
    data = [("0.0", 0), ("", None), ("stringsample", None), ("2.4", 2)]
    df = spark_session.createDataFrame(data, ["value", "expected_value"]).withColumn(
        "clean_value", CleanFunctions.clean_numeric(col("value"), "int")
    )
    print("helloworld")
    assert_column_equality(df, "clean_value", "expected_value")


@pytest.mark.usefixtures("spark_session")
def test_clean_string(spark_session):
    data = [("jose", "jose"), ("", None), (" ", None), ("luiz", "luiz")]
    df = spark_session.createDataFrame(data, ["name", "expected_name"]).withColumn(
        "clean_name", CleanFunctions.clean_string(col("name"))
    )

    assert_column_equality(df, "clean_name", "expected_name")
