# CS511_Project4
Author:
Zonglin Peng(zonglin7)

## Submission
* Google Drive [Link](https://drive.google.com/drive/folders/1ESwXogN9JaE93GE0HjGf4zN7yQScD4xE?usp=sharing) (access with @illinois email)
* Report: Google Doc [Link](https://docs.google.com/document/d/1HNopPgIyL_ha0OwhbVALGIX-qCNVZrnwBTMwq66c_NM/edit?usp=sharing)
* Video: Uploaded to Drive

## Description
This benchmark compares the read and write performance of SparkSQL and MySQL
[SparkSQL](./sparksql/README.md)
[MySQL](./mysql/README.md)

## Set up development environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e "."
```

## Run benchmark
```bash
sqlbenchmark
```
