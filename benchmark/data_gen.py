from os.path import expanduser, join, abspath, dirname
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pathlib import Path
import time 
from functools import wraps

DATA_PATH = abspath(join("asset", "kv", "data.txt"))

def main():
    '''
    TODO: Augment data size to ~1M entries
    '''
    raise NotImplementedError