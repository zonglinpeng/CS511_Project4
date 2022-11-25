from os.path import expanduser, join, abspath, dirname
from pathlib import Path
from sparksql.project4 import sparksql
from mysql.project4 import mysql


class Benchmark:
  def __init__(self) -> None:
    pass
  
  def run(self):
    sparksql.run()
    mysql.run()
  
def main():
  Benchmark().run()
  
if __name__ == "__main__":
  main()