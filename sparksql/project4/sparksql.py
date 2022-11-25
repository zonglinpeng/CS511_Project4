from os.path import expanduser, join, abspath, dirname
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pathlib import Path
import time 
from functools import wraps

DB_TYPE = "sparksql"
ROOT_DIR = Path(__file__).parent.parent.parent
DATA_PATH = abspath(join("asset", "kv", "sample_data.txt"))
WAREHOUSE_PATH = abspath(join(ROOT_DIR, "spark-warehouse"))
print(WAREHOUSE_PATH)

def timer(func):
  @wraps(func)
  def time_wrapper(*args, **kwargs):
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f'Function {func.__name__} Took {total_time:.4f} seconds')
    return result
  return time_wrapper


class SparkSQL:
  def __init__(self):
    self.spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive instance") \
        .config("spark.sql.warehouse.dir", WAREHOUSE_PATH) \
        .enableHiveSupport() \
        .getOrCreate()

  def create(self):
    self.spark.sql("CREATE TABLE IF NOT EXISTS src (key STRING, value STRING) USING hive")
    
  def stop(self):
    self.spark.stop()

  @timer
  def load_data(self):
    self.spark.sql(f"LOAD DATA LOCAL INPATH '{DATA_PATH}' INTO TABLE src") # TODO: benchmark

  @timer
  def query_data(self):
    self.spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key").show() # TODO: benchmark

def run():
  spark_sql = SparkSQL()
  try:
    spark_sql.create()
    spark_sql.load_data()
    spark_sql.query_data()
  except Exception as e:
    print(e)
  spark_sql.stop()
