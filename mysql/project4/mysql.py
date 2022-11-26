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
    return result
  return time_wrapper

class MySQL:
  def __init__(self):
    pass

  def create(self):
    pass
    
  def stop(self):
    pass

  @timer
  def load_data(self):
    pass

  @timer
  def query_data(self):
    pass

def run():
  return 0, 0
