from os.path import expanduser, join, abspath, dirname
from pathlib import Path
import time 
from functools import wraps

ROOT_DIR = Path(__file__).parent.parent.parent
DATA_PATH = abspath(join("asset", "kv", "data.txt"))
WAREHOUSE_PATH = abspath(join("asset", "spark-warehouse"))

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

class MySQL:
  def __init__(self):
    self.mysql = None # TODO MySQL connector

  def create(self):
    # "CREATE TABLE IF NOT EXISTS src (key STRING, value STRING)
    pass
  
  def stop(self):
    pass

  @timer
  def load_data(self):
    # LOAD DATA LOCAL INFILE '{DATA_PATH}' INTO TABLE src
    pass

  @timer
  def query_data(self):
    # SELECT * FROM src s1 JOIN src s2 WHERE s1.key > 1000 ORDER BY s1.key
    pass

def run():
  load_runtime, query_runtime = 0, 0
  my_sql = MySQL()
  try:
    my_sql.create()
    _, load_runtime = my_sql.load_data()
    _, query_runtime = my_sql.query_data()
  except Exception as e:
    print(e)
  my_sql.stop()
  return load_runtime, query_runtime

