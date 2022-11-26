from pathlib import Path
from os.path import expanduser, join, abspath, dirname

ROOT_DIR = Path(__file__).parent
DATA_STORE_PATH = abspath(join(ROOT_DIR, "asset", "data", "data.json"))
DATA_PATH = abspath(join("asset", "kv", "data.txt"))
WAREHOUSE_PATH = abspath(join(ROOT_DIR, "spark-warehouse"))
