from pyspark.sql import SparkSession



def get_spark_session():
    spark = SparkSession.builder \
        .appName("Desafio_Presto") \
        .master("local[*]")\
        .getOrCreate()
    return spark

