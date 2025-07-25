from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .appName("Cleaning") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()


df = spark.read.csv("hdfs://namenode:8020/data/bronze/2022/2022_bronx.csv", header=True, inferSchema=True)


df.show(5)