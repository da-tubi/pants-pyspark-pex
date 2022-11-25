import pandas as pd

# Declare the function and create the UDF
def multiply_func(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b

def run_pandas_udf(spark):
    from pyspark.sql.functions import col, pandas_udf
    from pyspark.sql.types import LongType
    print(f"We are using Pandas {pd.__version__}")
    multiply = pandas_udf(multiply_func, returnType=LongType())
    x = pd.Series([1, 2, 3])
    print(multiply_func(x, x))
    
    df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))
    df.select(multiply(col("x"), col("x"))).show()

if __name__ == "__main__":
    import os
    from pyspark.sql import SparkSession
    
    os.environ['PYSPARK_PYTHON'] = "./dist/app.pex"
    spark = SparkSession.builder.config(
        "spark.files",  # 'spark.yarn.dist.files' in YARN.
        "dist/app.pex"
    ).getOrCreate()
    run_pandas_udf(spark)
