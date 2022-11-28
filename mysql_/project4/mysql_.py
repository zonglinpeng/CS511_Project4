from os.path import expanduser, join, abspath, dirname
from pathlib import Path
import time 
from functools import wraps
import mysql.connector
from tabulate import tabulate

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
    self.mysql_ = mysql.connector.connect(
      user="root",
      database="project4",
      password="cs511-rubfish",
      allow_local_infile=True
    )
    cursor = self.mysql_.cursor()
    cursor.execute("SET GLOBAL local_infile=1")


  def create(self):
    # "CREATE TABLE IF NOT EXISTS src (k STRING, val STRING)
    cursor = self.mysql_.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS src")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS src (k INT, val varchar(255));")
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
  
  def stop(self):
    cursor = self.mysql_.cursor()
    cursor.execute("SET GLOBAL local_infile=0")
    self.mysql_.close()

  @timer
  def load_data(self):
    # LOAD DATA LOCAL INFILE '{DATA_PATH}' INTO TABLE src
    cursor = self.mysql_.cursor()
    query = (f"LOAD DATA LOCAL INFILE '{DATA_PATH}' INTO TABLE src FIELDS TERMINATED BY ''")
    cursor.execute(query)

  @timer
  def query_data(self):
    cursor = self.mysql_.cursor()
    query = ("SELECT * FROM src ORDER BY k")
    print("Executing Query")
    cursor.execute(query)
    table = [['key', 'value']]
    print("Fetching Data")
    data = cursor.fetchall()
    table.extend(data[:20])
    print(tabulate(table))


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

