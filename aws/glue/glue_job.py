import sys
import re
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp, lit


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_INPUT_PATH",
        "S3_OUTPUT_PATH",
        "USER_ID",
        "DOCUMENT_ID",
        "FILE_NAME",
        "FILE_TYPE"
    ]
)

spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

input_path = args["S3_INPUT_PATH"]
output_path = args["S3_OUTPUT_PATH"]

user_id = args["USER_ID"]
document_id = args["DOCUMENT_ID"]
file_name = args["FILE_NAME"]
file_type = args["FILE_TYPE"].lower()


def clean_column_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def read_structured_file(path: str, file_type: str):
    if file_type == "csv":
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )

    if file_type == "json":
        return (
            spark.read
            .option("multiLine", "true")
            .json(path)
        )

    if file_type == "parquet":
        return spark.read.parquet(path)

    raise Exception(
        f"Unsupported file type for Glue job: {file_type}. "
        "Currently supported: csv, json, parquet. "
        "For xlsx, convert to csv before Glue or add spark-excel dependency."
    )


df = read_structured_file(input_path, file_type)

for old_col in df.columns:
    df = df.withColumnRenamed(old_col, clean_column_name(old_col))

for c in df.columns:
    df = df.withColumn(c, trim(col(c).cast("string")))

df = df.dropna(how="all")
df = df.dropDuplicates()

df = df.withColumn("user_id", lit(user_id))
df = df.withColumn("document_id", lit(document_id))
df = df.withColumn("source_file_name", lit(file_name))
df = df.withColumn("processed_at", current_timestamp())

df.write.mode("overwrite").parquet(output_path)

print("Glue structured ETL completed successfully")
print("Input path:", input_path)
print("Output path:", output_path)
print("User ID:", user_id)
print("Document ID:", document_id)
print("File name:", file_name)
print("Rows processed:", df.count())