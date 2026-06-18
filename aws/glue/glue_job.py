import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_INPUT_PATH",
        "S3_OUTPUT_PATH"
    ]
)

spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

input_path = args["S3_INPUT_PATH"]
output_path = args["S3_OUTPUT_PATH"]

df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

for old_col in df.columns:
    new_col = old_col.strip().lower().replace(" ", "_")
    df = df.withColumnRenamed(old_col, new_col)

for c in df.columns:
    df = df.withColumn(c, trim(col(c).cast("string")))

df = df.dropDuplicates()
df = df.withColumn("processed_at", current_timestamp())

df.write.mode("overwrite").parquet(output_path)

print("Glue job completed successfully")