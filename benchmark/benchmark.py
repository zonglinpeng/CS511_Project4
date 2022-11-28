from os.path import expanduser, join, abspath, dirname
from sparksql.project4 import sparksql
from mysql_.project4 import mysql_
from .datagen import datagen
from math import log10
import matplotlib.pyplot as plt
from tabulate import tabulate


class Benchmark:
  def __init__(self) -> None:
    self.data_size = [10**i for i in range(1, 7)]
  
  def run(self):
    spark_load_data, my_load_data = [], []
    spark_query_data, my_query_data = [], []
    for size in self.data_size:
      datagen(size)
      spark_load_runtime = 0
      spark_query_runtime = 0
      mysql_load_runtime = 0
      mysql_query_runtime = 0
      for i in range(5):
        load_runtime, query_runtime = sparksql.run()
        spark_load_runtime += load_runtime
        spark_query_runtime += query_runtime

        load_runtime, query_runtime = mysql_.run()
        mysql_load_runtime += load_runtime
        mysql_query_runtime += query_runtime
      
      spark_load_data.append(round(spark_load_runtime/5, 4))
      spark_query_data.append(round(spark_query_runtime/5, 4))
      my_load_data.append(round(mysql_load_runtime/5, 4))
      my_query_data.append(round(mysql_query_runtime/5, 4))

    x = [int(log10(size)) for size in self.data_size]
    title = ["Data Size"]
    title.extend(['10^' + str(_) for _ in x])
    table = [title]
    rowNames = ["SparkSQL Load Time", "MySQL Load Time", "SparkSQL Query Time", "MySQL Query Time"]
    runtime = [spark_load_data, my_load_data, spark_query_data, my_query_data]
    for i, row in enumerate(rowNames):
      l = [row]
      l.extend(runtime[i])
      table.append(l)
    print(tabulate(table))

    plt.plot(x, spark_load_data, '--o', label="SparkSQL")
    plt.plot(x, my_load_data, '--o', label="MySQL")
    plt.title("SQL Load Data")
    plt.xlabel('10^N data entries') 
    plt.ylabel('Overhead(seconds)') 
    plt.legend()
    plt.show()
    
    plt.plot(x, spark_query_data, '--o', label="SparkSQL")
    plt.plot(x, my_query_data, '--o', label="MySQL")
    plt.title("SQL Query Data")
    plt.xlabel('10^N data entries') 
    plt.ylabel('Overhead(seconds)') 
    plt.legend()
    plt.show()
    
    
def main():
  Benchmark().run()
  
if __name__ == "__main__":
  main()