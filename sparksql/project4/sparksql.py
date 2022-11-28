from os.path import expanduser, join, abspath, dirname
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pathlib import Path
import time 
import json
from functools import wraps
from config import DATA_PATH, DATA_STORE_PATH, WAREHOUSE_PATH

DB_TYPE = "sparksql"


def timer(func):
  @wraps(func)
  def time_wrapper(*args, **kwargs):
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f'Function {func.__name__} Took {total_time:.4f} seconds')
    return result, total_time
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
    self.spark.sql("DROP TABLE IF EXISTS src")
    self.spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    
  def stop(self):
    self.spark.stop()

  @timer
  def load_data(self):
    self.spark.sql(f"LOAD DATA LOCAL INPATH '{DATA_PATH}' INTO TABLE src")

  @timer
  def query_data(self):
    self.spark.sql("SELECT * FROM src ORDER BY key").show()

def run():
  load_runtime, query_runtime = 0, 0
  spark_sql = SparkSQL()
  try:
    spark_sql.create()
    _, load_runtime = spark_sql.load_data()
    _, query_runtime = spark_sql.query_data()
  except Exception as e:
    print(e)
  spark_sql.stop()
  return load_runtime, query_runtime
