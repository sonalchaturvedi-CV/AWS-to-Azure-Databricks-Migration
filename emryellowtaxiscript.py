#import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Column Partition Creation") \
    .getOrCreate()

# Specify the database and table names
database_name = "nyctaxiyellowdb"
table_name = "sonalaws_glue_2023"
new_table_name = "yellowtaxidatapartitioned"

# Read the table from the AWS Glue Data Catalog
df = spark.table(f"{table_name}")

# Add the year and month columns
df_with_columns = df.withColumn("year", year(df["tpep_pickup_datetime"])) \
                    .withColumn("month", month(df["tpep_pickup_datetime"]))

# Define the partition columns
partition_columns = ["year", "month"]

# Save the DataFrame as a partitioned table
df_with_columns.write.mode("overwrite").format("parquet") \
    .partitionBy(partition_columns).saveAsTable(f"{database_name}.{new_table_name}")

spark.stop()
