import time
from functools import wraps
from os.path import abspath, dirname, expanduser, join
from pathlib import Path

from pymysql import connect

ROOT_DIR = Path(__file__).parent.parent.parent
DATA_PATH = abspath(join("asset", "kv", "data.txt"))
WAREHOUSE_PATH = abspath(join("asset", "spark-warehouse"))

DB_CONFIG = {
    'host':'localhost',
    'user': 'root',
    'password': 'weikunwu0314',
    'local_infile': 'True'
}

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
    db = connect(**DB_CONFIG)
    db.select_db("project4")
    cursor = db.cursor()
    cursor.execute("SET GLOBAL local_infile=1")
    db.commit()
    self.mysql = db

  def create(self):
    # "CREATE TABLE IF NOT EXISTS src (key STRING, value STRING)
    cursor = self.mysql.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS src (`key` INT, value VARCHAR(255))")
    self.mysql.commit()
  
  def stop(self):
    self.mysql.close()

  @timer
  def load_data(self):
    # LOAD DATA LOCAL INFILE '{DATA_PATH}' INTO TABLE src
    cursor = self.mysql.cursor()
    cursor.execute(f"LOAD DATA LOCAL INFILE '{DATA_PATH}' INTO TABLE src")
    self.mysql.commit()


  @timer
  def query_data(self):
    # SELECT * FROM src s1 JOIN src s2 WHERE s1.key > 1000 ORDER BY s1.key
    self.mysql.cursor().execute("SELECT * FROM src s1 JOIN src s2 WHERE s1.key > 1000 ORDER BY s1.key")

def run():
  load_runtime, query_runtime = 0, 0
  print("Running MySQL")
  my_sql = MySQL()
  try:
    my_sql.create()
    _, load_runtime = my_sql.load_data()
    _, query_runtime = my_sql.query_data()
  except Exception as e:
    print(e)
  my_sql.stop()
  return load_runtime, query_runtime

