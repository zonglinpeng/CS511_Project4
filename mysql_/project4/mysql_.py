from os.path import expanduser, join, abspath, dirname
from pathlib import Path
import time 
from functools import wraps
import mysql.connector

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

class MySQL_:
  def __init__(self):
    self.mysql_ = mysql.connector.connect(
      user="root",
      database="mysql",
      allow_local_infile=True
    )

  def create(self):
    # "CREATE TABLE IF NOT EXISTS src (i_key STRING, j_value STRING)
    cursor = self.mysql_.cursor()
    try:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS SRC (i_key varchar(255), j_value varchar(255));")
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
    # pass
  
  def stop(self):
    pass

  @timer
  def load_data(self):
    # LOAD DATA LOCAL INFILE '{DATA_PATH}' INTO TABLE src
    cursor = self.mysql_.cursor()
    query = (f"LOAD DATA LOCAL INFILE '{DATA_PATH}' INTO TABLE src")
    cursor.execute(query)
    pass

  @timer
  def query_data(self):
    cursor = self.mysql_.cursor()
    query = ("SELECT * FROM src s1 JOIN src s2 WHERE s1.i_key > 1000 ORDER BY s1.i_key")
    cursor.execute(query)

  def stop(self):
    self.mysql_.close()
    # SELECT * FROM src s1 JOIN src s2 WHERE s1.i_key > 1000 ORDER BY s1.i_key
    # pass

def run():
  load_runtime, query_runtime = 0, 0
  my_sql = MySQL_()
  try:
    my_sql.create()
    _, load_runtime = my_sql.load_data()
    _, query_runtime = my_sql.query_data()
    my_sql.stop()
  except Exception as e:
    print(e)
    
  my_sql.stop()
  return load_runtime, query_runtime

