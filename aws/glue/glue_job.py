import sys
import re

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, current_timestamp, lit
from pyspark.sql.types import IntegerType


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_INPUT_PATH",
        "TABLE_NAME",
        "USER_ID",
        "DOCUMENT_ID",
        "FILE_NAME",
        "PG_HOST",
        "PG_PORT",
        "PG_DB",
        "PG_USER",
        "PG_PASSWORD",
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# s3_input_path = args["S3_INPUT_PATH"]
table_name = args["TABLE_NAME"]

user_id = args["USER_ID"]
document_id = args["DOCUMENT_ID"]
file_name = args["FILE_NAME"]

pg_host = args["PG_HOST"]
pg_port = args["PG_PORT"]
pg_db = args["PG_DB"]
pg_user = args["PG_USER"]
pg_password = args["PG_PASSWORD"]


def clean_column_name(name):
    name = str(name).strip().lower()
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_") or "column"


def validate_data_quality(df, file_name):
    total_rows = df.count()

    if total_rows == 0:
        raise Exception(f"File {file_name} has 0 rows after cleaning. Rejecting.")

    for field in df.schema.fields:
        null_count = df.filter(col(field.name).isNull()).count()
        null_pct = null_count / total_rows if total_rows > 0 else 0

        if null_pct > 0.9:
            print(f"[WARNING] Column '{field.name}' is {null_pct * 100:.0f}% null")

    print(f"[glue] Data quality check passed: {total_rows} rows")
    return df


print("Starting direct S3 to RDS Glue ETL")
print("Input:", s3_input_path)
print("Target Table:", table_name)
print("User ID:", user_id)
print("Document ID:", document_id)
print("File Name:", file_name)

file_ext = s3_input_path.lower().split(".")[-1]

if file_ext == "csv":
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(s3_input_path)
    )

elif file_ext == "json":
    df = (
        spark.read
        .option("multiLine", "true")
        .json(s3_input_path)
    )

elif file_ext == "parquet":
    df = spark.read.parquet(s3_input_path)

else:
    raise Exception(f"Unsupported structured file type: {file_ext}")


for old_col in df.columns:
    df = df.withColumnRenamed(old_col, clean_column_name(old_col))

for field in df.schema.fields:
    if field.dataType.simpleString() == "string":
        df = df.withColumn(
            field.name,
            trim(col(field.name))
        )

df = df.dropna(how="all")
df = df.dropDuplicates()
df = validate_data_quality(df, file_name)

df = df.withColumn(
    "user_id",
    lit(int(user_id)).cast(IntegerType())
)

df = df.withColumn(
    "document_id",
    lit(int(document_id)).cast(IntegerType())
)

df = df.withColumn("source_file_name", lit(file_name))
df = df.withColumn("processed_at", current_timestamp())

row_count = df.count()

print("Rows:", row_count)
print("Columns:", len(df.columns))
print("Column Names:", df.columns)

print("Final Schema:")
df.printSchema()

jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

print("Writing to RDS PostgreSQL...")
print("JDBC URL:", jdbc_url)
print("Table:", table_name)

(
    df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", table_name)
    .option("user", pg_user)
    .option("password", pg_password)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)


print("Glue ETL completed successfully")
print("Input:", s3_input_path)
print("RDS table:", table_name)
print("Rows processed:", row_count)