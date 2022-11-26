import sys
from os.path import expanduser, join, abspath, dirname
from random import shuffle
from functools import wraps

DEFAULT_DATA_LENGTH = 10**3
DATA_PATH = abspath(join("asset", "kv", "data.txt"))

def datagen(length):
    datalist = [f"{i}{chr(1)}{i}_val\n" for i in range(length)]
    shuffle(datalist)
    with open(DATA_PATH, "w+") as f:
        for data in datalist:
            f.write(data)
            
def main():
    length = int(sys.argv[1]) if sys.argv[1] else DEFAULT_DATA_LENGTH
    datagen(length)
    
if __name__ == "__main__":
    main()