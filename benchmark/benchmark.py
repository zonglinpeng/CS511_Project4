from os.path import expanduser, join, abspath, dirname
from sparksql.project4 import sparksql
from mysql_.project4 import mysql_
from .datagen import datagen
from math import log10
import matplotlib.pyplot as plt


class Benchmark:
  def __init__(self) -> None:
    self.data_size = [10**i for i in range(1, 6)]
  
  def run(self):
    spark_load_data, my_load_data = [], []
    spark_query_data, my_query_data = [], []
    for size in self.data_size:
      datagen(size)
      load_runtime, query_runtime = sparksql.run()
      spark_load_data.append(load_runtime)
      spark_query_data.append(query_runtime)
      load_runtime, query_runtime = mysql_.run()
      my_load_data.append(load_runtime)
      my_query_data.append(query_runtime)
    
    x = [int(log10(size)) for size in self.data_size]
    
    plt.plot(x, spark_load_data, '--o', label="SparkSQL")
    plt.plot(x, my_load_data, '--o', label="MykSQL")
    plt.title("SQL Load Data")
    plt.xlabel('10^N data entres') 
    plt.ylabel('Overhead(seconds)') 
    plt.legend()
    plt.show()
    
    plt.plot(x, spark_query_data, '--o', label="SparkSQL")
    plt.plot(x, my_query_data, '--o', label="MykSQL")
    plt.title("SQL Query Data")
    plt.xlabel('10^N data entres') 
    plt.ylabel('Overhead(seconds)') 
    plt.legend()
    plt.show()
    
    
def main():
  Benchmark().run()
  
if __name__ == "__main__":
  main()