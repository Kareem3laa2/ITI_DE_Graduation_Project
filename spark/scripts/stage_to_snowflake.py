import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,TimestampType
from dotenv import load_dotenv



spark = SparkSession.builder \
    .appName("NYC_Snowflake_Staging") \
    .master("spark://nyc-spark-master:7077") \
    .getOrCreate()


schema = StructType([
    StructField("neighborhood", StringType(), True),
    StructField("building_class_category", StringType(), True),
    StructField("tax_class_at_present", StringType(), True),
    StructField("block", IntegerType(), True),
    StructField("lot", IntegerType(), True),
    StructField("building_class_at_present", StringType(), True),
    StructField("address", StringType(), True),
    StructField("zip_code", IntegerType(), True),
    StructField("residential_units", IntegerType(), True),
    StructField("commercial_units", StringType(), True),  # still string as in log
    StructField("total_units", IntegerType(), False),     # nullable = False
    StructField("land_square_feet", DoubleType(), True),
    StructField("gross_square_feet", DoubleType(), True),
    StructField("tax_class_at_time_of_sale", IntegerType(), True),
    StructField("building_class_at_time_of_sale", StringType(), True),
    StructField("sale_price", DoubleType(), True),
    StructField("sale_date", TimestampType(), True),
    StructField("year_built", IntegerType(), True),
    StructField("borough_name", StringType(), False)      # nullable = False
])


print("=== ENVIRONMENT VARIABLES DEBUG ===")
print(f"SNOWFLAKE_ACCOUNT: {os.getenv('SNOWFLAKE_ACCOUNT')}")
print(f"SNOWFLAKE_USER: {os.getenv('SNOWFLAKE_USER')}")
print(f"SNOWFLAKE_DATABASE: {os.getenv('SNOWFLAKE_DATABASE')}")
print(f"SNOWFLAKE_SCHEMA: {os.getenv('SNOWFLAKE_SCHEMA')}")
print(f"SNOWFLAKE_WAREHOUSE: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
print(f"SNOWFLAKE_ROLE: {os.getenv('SNOWFLAKE_ROLE')}")
print(f"SNOWFLAKE_PASSWORD: {os.getenv('SNOWFLAKE_PASSWORD')}")




df = spark.read.csv("hdfs://namenode:8020/data/silver/nyc_silver.csv", header=True, schema = schema)



df.write \
  .format("snowflake") \
  .option("sfURL", os.getenv('SNOWFLAKE_ACCOUNT')) \
  .option("sfUser", os.getenv("SNOWFLAKE_USER")) \
  .option("sfPassword", os.getenv("SNOWFLAKE_PASSWORD")) \
  .option("sfDatabase", os.getenv("SNOWFLAKE_DATABASE")) \
  .option("sfSchema", os.getenv("SNOWFLAKE_SCHEMA")) \
  .option("sfWarehouse", os.getenv("SNOWFLAKE_WAREHOUSE")) \
  .option("dbtable", "SILVER_NYC_PROPERTIES") \
  .option("sfRole", os.getenv("SNOWFLAKE_ROLE")) \
  .mode("overwrite") \
  .save()