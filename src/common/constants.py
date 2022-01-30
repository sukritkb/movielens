import enum


class JobType(enum.Enum):
    Staging = "staging"
    Transformation = "transformation"

class DataFormats(enum.Enum):
    DELTA = "delta"
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"


DEFAULT_READ_OPTIONS = {"mergeSchema": "true"}
DEFAULT_WRITE_OPTIONS = {"mergeSchema": "true"}
DEFAULT_SPARK_CONF = [("spark.driver.memory", "1g"),("spark.executor.memory","1g")]
    
