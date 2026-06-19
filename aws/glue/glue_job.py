import sys
import re

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, current_timestamp, lit


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DATABASE_NAME",
        "TABLE_NAME",
        "S3_OUTPUT_PATH",
        "USER_ID",
        "DOCUMENT_ID",
        "FILE_NAME"
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

database_name = args["DATABASE_NAME"]
table_name = args["TABLE_NAME"]
output_path = args["S3_OUTPUT_PATH"]
user_id = args["USER_ID"]
document_id = args["DOCUMENT_ID"]
file_name = args["FILE_NAME"]


def clean_column_name(name):
    name = str(name).strip().lower()
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name
)

df = dyf.toDF()

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

print("Glue ETL from Data Catalog completed successfully")
print("Database:", database_name)
print("Table:", table_name)
print("Output:", output_path)
print("Rows processed:", df.count())