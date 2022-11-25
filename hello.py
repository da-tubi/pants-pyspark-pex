import os
import sys

import pandas as pd

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType
from pyspark.sql import SparkSession


# Declare the function and create the UDF
def multiply_func(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b

def run_pandas_udf(spark):
    print(f"We are using Pandas {pd.__version__}")
    multiply = pandas_udf(multiply_func, returnType=LongType())
    x = pd.Series([1, 2, 3])
    print(multiply_func(x, x))
    
    df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))
    df.select(multiply(col("x"), col("x"))).show()

def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    spark = SparkSession.builder.getOrCreate()
    run_pandas_udf(spark)

if __name__ == "__main__":
    main()
