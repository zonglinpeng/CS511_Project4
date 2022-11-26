import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="sqlbenchmark",
    version="0.0.1",
    author="Zonglin Peng",
    description=("A framework to run sql"),
    python_requires=">=3.7,",
    packages=["."],
    include_package_data=True,
    install_requires=["pyspark==3.3.1", "matplotlib", "black"],
    long_description=read("README.md"),
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    entry_points={
        "console_scripts": [
            "sqlbenchmark = benchmark.benchmark:main",
            "datagen = benchmark.datagen:main"
        ]
    },
)