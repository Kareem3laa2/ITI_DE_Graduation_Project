from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window



spark = SparkSession.builder\
         .appName("SilverCleansing")\
         .master("spark://spark-master:7077")\
         .getOrCreate()




## Define the schema for the data
schema = StructType([
    StructField("borough", StringType(), True),
    StructField("neighborhood", StringType(), True),
    StructField("building_class_category", StringType(), True),
    StructField("tax_class_at_present", StringType(), True),
    StructField("block", StringType(), True),
    StructField("lot", StringType(), True),
    StructField("ease_ment", StringType(), True),
    StructField("building_class_at_present", StringType(), True),
    StructField("address", StringType(), True),
    StructField("apartment_number", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("residential_units", StringType(), True),
    StructField("commercial_units", StringType(), True),
    StructField("total_units", StringType(), True),
    StructField("land_square_feet", DoubleType(), True),  # will clean later
    StructField("gross_square_feet", DoubleType(), True),
    StructField("year_built", StringType(), True),
    StructField("tax_class_at_time_of_sale", StringType(), True),
    StructField("building_class_at_time_of_sale", StringType(), True),
    StructField("sale_price", DoubleType(), True),         # will convert to numeric
    StructField("sale_date", StringType(), True)           # will parse to date
])


## Read the data
df = (
    spark.read.option("header", True)
               .option("inferSchema", True)
               .csv(["hdfs://namenode:8020/data/bronze/2022", "hdfs://namenode:8020/data/bronze/2023", "hdfs://namenode:8020/data/bronze/2024"])
               .drop("apartment_number", "easement")
)


## Fix some column types

columns_to_cast = ["zip_code", "block", "lot", "residential_units", "commercial_units","year_built","tax_class_at_time_of_sale"]
for col_name in columns_to_cast:
    df = df.withColumn(
        col_name,
        regexp_replace(col(col_name), r"\.0$", "").cast("int")
    )

## Fixing the missing of total_units = resdential_units + commercial_units
df = df.withColumn(
    "total_units",
    coalesce(col("residential_units"), lit(0)) + coalesce(col("commercial_units"), lit(0))
)

## Imputing for the residential units NULLS
df = df.withColumn(
    "residential_units",
    when(
        ((col("residential_units").isNull()) | (col("residential_units") == 0)) &
        (col("building_class_category") == "01 ONE FAMILY DWELLINGS"),
        1
    ).when(
        ((col("residential_units").isNull()) | (col("residential_units") == 0)) &
        (col("building_class_category") == "02 TWO FAMILY DWELLINGS"),
        2
    ).when(
        ((col("residential_units").isNull()) | (col("residential_units") == 0)) &
        (col("building_class_category").isin(
            "08 RENTALS - ELEVATOR APARTMENTS",
            "09 COOPS - WALKUP APARTMENTS",
            "10 COOPS - ELEVATOR APARTMENTS"
        )) &
        (col("total_units").isNotNull()),
        col("total_units")
    ).when(
        ((col("residential_units").isNull()) | (col("residential_units") == 0)) &
        (col("building_class_category").isin(
            "17 CONDO COOPS",
            "49 CONDO WAREHOUSES/FACTORY/INDUS",
            "28 COMMERCIAL CONDOS",
            "47 CONDO NON-BUSINESS STORAGE",
            "48 CONDO TERRACES/GARDENS/CABANAS",
            "44 CONDO PARKING",
            "43 CONDO OFFICE BUILDINGS",
            "45 CONDO HOTELS",
            "41 TAX CLASS 4 - OTHER",
            "46 CONDO STORE BUILDINGS",
            "21 OFFICE BUILDINGS",
            "42 CONDO CULTURAL/MEDICAL/EDUCATIONAL/ETC"
        )), 
        0
    ).otherwise(col("residential_units"))
)

## Imputing for the commercial_units NULLS
df = df.withColumn(
    "commercial_units",
    when(
        (col("commercial_units").isNull()) &
        (col("building_class_category").isin(
            "01 ONE FAMILY DWELLINGS",
            "02 TWO FAMILY DWELLINGS",
            "03 THREE FAMILY DWELLINGS",
            "08 RENTALS - ELEVATOR APARTMENTS",
            "09 COOPS - WALKUP APARTMENTS",
            "10 COOPS - ELEVATOR APARTMENTS",
            "12 CONDOS - WALKUP APARTMENTS",
            "13 CONDOS - ELEVATOR APARTMENTS",
            "15 CONDOS - 2-10 UNIT RESIDENTIAL",
            "04 TAX CLASS 1 CONDOS",
            "11 SPECIAL CONDO BILLING LOTS",
            "17 CONDO COOPS"
        )),
        "0.0"
    ).when(
        (col("commercial_units").isNull()) &
        (col("building_class_category") == "16 CONDOS - 2-10 UNIT WITH COMMERCIAL UNIT"),
        "1.0"
    ).when(
        (col("commercial_units").isNull()) &
        (col("building_class_category") == "21 OFFICE BUILDINGS") &
        (col("total_units").isNotNull()),
        col("total_units")
    ).otherwise(col("commercial_units"))
)


## Smart Imputation for land_square_feet
def smart_imputation(df):
    # List of building class categories where land_square_feet is typically irrelevant
    zero_land_categories = [
        "CONDO", "COOPS", "CONDO PARKING", "CONDO NON-BUSINESS STORAGE",
        "CONDO OFFICE BUILDINGS", "CONDO STORE BUILDINGS", "CONDO HOTELS",
        "CONDO TERRACES/GARDENS/CABANAS", "CONDO WAREHOUSES/FACTORY/INDUS",
        "CONDO CULTURAL/MEDICAL/EDUCATIONAL/ETC"
    ]

    # Create boolean column to identify condo-like properties
    df = df.withColumn(
        "is_condo_type",
        col("building_class_category").rlike("(?i)" + "|".join(zero_land_categories))
    )

    # Define windows with increasing generalization
    window_detailed = Window.partitionBy("building_class_category", "borough", "neighborhood")
    window_medium = Window.partitionBy("building_class_category", "borough")
    window_basic = Window.partitionBy("building_class_category")

    # Calculate medians
    df = (df
        .withColumn("median_gross_detailed", expr("percentile_approx(gross_square_feet, 0.5)").over(window_detailed))
        .withColumn("median_gross_medium", expr("percentile_approx(gross_square_feet, 0.5)").over(window_medium))
        .withColumn("median_gross_basic", expr("percentile_approx(gross_square_feet, 0.5)").over(window_basic))
        .withColumn("median_land_detailed", expr("percentile_approx(land_square_feet, 0.5)").over(window_detailed))
        .withColumn("median_land_medium", expr("percentile_approx(land_square_feet, 0.5)").over(window_medium))
        .withColumn("median_land_basic", expr("percentile_approx(land_square_feet, 0.5)").over(window_basic))
    )

    # Set flags BEFORE imputation (based on current nulls)
    df = df.withColumn("gross_sqft_imputed_flag", col("gross_square_feet").isNull())
    df = df.withColumn("land_sqft_imputed_flag", col("land_square_feet").isNull())

    # Impute gross_square_feet
    df = df.withColumn(
        "gross_square_feet",
        when(col("gross_square_feet").isNotNull(), col("gross_square_feet"))
        .when(col("median_gross_detailed").isNotNull(), col("median_gross_detailed"))
        .when(col("median_gross_medium").isNotNull(), col("median_gross_medium"))
        .otherwise(col("median_gross_basic"))
    )

    # Impute land_square_feet with conditional logic for condo-like categories
    df = df.withColumn(
        "land_square_feet",
        when(col("land_square_feet").isNotNull(), col("land_square_feet"))
        .when(col("is_condo_type"), lit(0))  # For condo/coops etc.
        .when(col("median_land_detailed").isNotNull(), col("median_land_detailed"))
        .when(col("median_land_medium").isNotNull(), col("median_land_medium"))
        .otherwise(col("median_land_basic"))
    )

    # Clean up helper columns
    df = df.drop(
        "median_gross_detailed", "median_gross_medium", "median_gross_basic",
        "median_land_detailed", "median_land_medium", "median_land_basic",
        "is_condo_type"
    )

    return df

df_processed = smart_imputation(df)


## Smart Imputation for gross_square_feet
def impute_gross_square_feet(df):
    # 1️⃣ Special case: Impute zero for non-residential condo types
    df = df.withColumn(
        "gross_square_feet",
        when(
            col("building_class_category").isin(
                "44 CONDO PARKING",
                "47 CONDO NON-BUSINESS STORAGE",
                "48 CONDO TERRACES/GARDENS/CABANAS"
            ),
            lit(0)
        ).otherwise(col("gross_square_feet"))
    )

    # 2️⃣ Smart imputation by neighborhood + building class
    window_neigh_class = Window.partitionBy("neighborhood", "building_class_category")
    df = df.withColumn(
        "median_gross_local",
        expr("percentile_approx(gross_square_feet, 0.5)").over(window_neigh_class)
    )
    df = df.withColumn(
        "gross_square_feet",
        when(col("gross_square_feet").isNotNull(), col("gross_square_feet"))
        .otherwise(col("median_gross_local"))
    ).drop("median_gross_local")

    # 3️⃣ Fallback imputation by building_class_category only
    window_class = Window.partitionBy("building_class_category")
    df = df.withColumn(
        "median_gross_category",
        expr("percentile_approx(gross_square_feet, 0.5)").over(window_class)
    )
    df = df.withColumn(
        "gross_square_feet",
        when(col("gross_square_feet").isNotNull(), col("gross_square_feet"))
        .otherwise(col("median_gross_category"))
    ).drop("median_gross_category")

    return df

df_processed = impute_gross_square_feet(df_processed)


## Smart Imputation for Year_built
# Step 1: Impute based on (borough, neighborhood, building_class_category)
w1 = Window.partitionBy("borough", "neighborhood", "building_class_category")
df_processed = df_processed.withColumn("year_built_lvl1", round(avg("year_built").over(w1)))

# Step 2: Impute based on (borough, building_class_category)
w2 = Window.partitionBy("borough", "building_class_category")
df_processed = df_processed.withColumn("year_built_lvl2", round(avg("year_built").over(w2)))

# Step 3: Impute based on (building_class_category)
w3 = Window.partitionBy("building_class_category")
df_processed = df_processed.withColumn("year_built_lvl3", round(avg("year_built").over(w3)))

# Step 4: Overall median as fallback
overall_median = df_processed.approxQuantile("year_built", [0.5], 0.01)[0]

# Step 5: Apply hierarchical imputation
df_processed = df_processed.withColumn(
    "year_built_imputed",
    when(col("year_built").isNotNull(), col("year_built"))
    .when(col("year_built_lvl1").isNotNull(), col("year_built_lvl1"))
    .when(col("year_built_lvl2").isNotNull(), col("year_built_lvl2"))
    .when(col("year_built_lvl3").isNotNull(), col("year_built_lvl3"))
)

# Optional: Replace original year_built column
df_processed = df_processed.drop("year_built")

df_processed = df_processed.withColumn(
    "year_built", 
    col("year_built_imputed").cast(IntegerType())
)

# Clean up helper columns
df_processed = df_processed.drop("year_built_lvl1", "year_built_lvl2", "year_built_lvl3")


## Dropping the nulls in these columns ( < %5 ) of the data
df_cleaned = df_processed.na.drop(subset=["tax_class_at_present", "building_class_at_present", "zip_code","tax_class_at_time_of_sale", "building_class_at_time_of_sale", "sale_price", "sale_date"])


## Replacing borough number with relevant names
df_cleaned= df_cleaned.withColumn("borough_name",
    when(col("borough") == 1.0, "MANHATTAN")
    .when(col("borough") == 2.0, "BRONX")
    .when(col("borough") == 3.0, "BROOKLYN")
    .when(col("borough") == 4.0, "QUEENS")
    .when(col("borough") == 5.0, "STATEN ISLAND")
    .otherwise("UNKNOWN")
).drop("borough","year_built_imputed","land_sqft_imputed_flag","gross_sqft_imputed_flag")


## Making sure there is no nulls
null_count = df_cleaned.select([count(when(col(c).isNull(), c)).alias(c) for c in df_cleaned.columns])
null_count.show()

df_cleaned.printSchema()

## Writing silver_version back to HDFS
df_cleaned.write.mode("overwrite").csv("hdfs://namenode:8020/data/silver/nyc_silver.csv")



 
