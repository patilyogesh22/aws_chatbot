import sys
import re
import json

import psycopg2
from psycopg2.extras import Json

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
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

s3_input_path = args["S3_INPUT_PATH"]
user_id = args["USER_ID"]
document_id = args["DOCUMENT_ID"]
file_name = args["FILE_NAME"]

pg_host = args["PG_HOST"]
pg_port = args["PG_PORT"]
pg_db = args["PG_DB"]
pg_user = args["PG_USER"]
pg_password = args["PG_PASSWORD"]

DSN = f"host={pg_host} port={pg_port} dbname={pg_db} user={pg_user} password={pg_password}"
JDBC_URL = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"


def clean_column_name(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def clean_table_stem(name: str) -> str:
    name = name.replace("raw_", "", 1)
    name = re.sub(r"\.[^.]+$", "", name)
    name = clean_column_name(name)
    return name or "dataset"


file_stem = clean_table_stem(args["TABLE_NAME"])
table_name = f"u{user_id}_d{document_id}_{file_stem}"

print("=" * 60)
print("[glue] Starting structured ETL")
print("[glue] USER_ID    :", user_id)
print("[glue] DOCUMENT_ID:", document_id)
print("[glue] FILE_NAME  :", file_name)
print("[glue] TABLE_NAME :", table_name)
print("[glue] S3 INPUT   :", s3_input_path)
print("=" * 60)


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
        df = df.withColumn(field.name, trim(col(field.name)))

df = df.dropna(how="all")
df = df.dropDuplicates()

df = df.withColumn("user_id", lit(int(user_id)).cast(IntegerType()))
df = df.withColumn("document_id", lit(int(document_id)).cast(IntegerType()))
df = df.withColumn("source_file_name", lit(file_name))
df = df.withColumn("processed_at", current_timestamp())

row_count = df.count()

print("[glue] Rows after cleaning:", row_count)
print("[glue] Columns:", df.columns)
df.printSchema()


print("[glue] Writing to RDS table:", table_name)

(
    df.write
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", table_name)
    .option("user", pg_user)
    .option("password", pg_password)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

print("[glue] RDS write complete")


GLUE_INTERNAL_COLS = {
    "user_id",
    "document_id",
    "source_file_name",
    "processed_at",
}


def get_pg_schema(tname: str) -> dict:
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                  AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (tname,))

            return {
                r[0]: r[1]
                for r in cur.fetchall()
                if r[0] not in GLUE_INTERNAL_COLS
            }
    finally:
        conn.close()


def get_sample_rows(tname: str, limit: int = 5) -> list:
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{tname}" LIMIT %s', (limit,))
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

        result = []

        for row in rows:
            item = {}

            for key, value in zip(cols, row):
                if key in GLUE_INTERNAL_COLS:
                    continue

                item[key] = None if value is None else str(value)

            result.append(item)

        return result

    except Exception as e:
        print("[glue] Sample rows error:", e)
        return []

    finally:
        conn.close()


schema_json = get_pg_schema(table_name)
sample_json = get_sample_rows(table_name, limit=5)

print("[glue] Schema columns:", list(schema_json.keys()))
print("[glue] Sample rows:", len(sample_json))


def update_structured_datasets():
    conn = psycopg2.connect(DSN)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE structured_datasets
                ADD COLUMN IF NOT EXISTS table_name TEXT,
                ADD COLUMN IF NOT EXISTS schema_json JSONB,
                ADD COLUMN IF NOT EXISTS sample_json JSONB,
                ADD COLUMN IF NOT EXISTS row_count INTEGER,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
            """)

            cur.execute("""
                UPDATE structured_datasets
                SET
                    table_name = %s,
                    schema_json = %s,
                    sample_json = %s,
                    row_count = %s,
                    status = 'ready',
                    updated_at = NOW()
                WHERE user_id = %s
                  AND document_id = %s
            """, (
                table_name,
                Json(schema_json),
                Json(sample_json),
                row_count,
                int(user_id),
                int(document_id),
            ))

            rows_updated = cur.rowcount
            print("[glue] structured_datasets rows updated:", rows_updated)

            if rows_updated == 0:
                cur.execute("""
                    INSERT INTO structured_datasets
                    (
                        user_id,
                        document_id,
                        file_name,
                        raw_s3_key,
                        table_name,
                        schema_json,
                        sample_json,
                        row_count,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'ready', NOW(), NOW())
                    ON CONFLICT DO NOTHING
                """, (
                    int(user_id),
                    int(document_id),
                    file_name,
                    s3_input_path,
                    table_name,
                    Json(schema_json),
                    Json(sample_json),
                    row_count,
                ))

        conn.commit()
        print("[glue] structured_datasets updated successfully")

    except Exception as e:
        conn.rollback()
        print("[glue] ERROR updating structured_datasets:", e)

    finally:
        conn.close()


update_structured_datasets()

print("=" * 60)
print("[glue] ETL complete")
print("[glue] RDS table:", table_name)
print("[glue] Rows:", row_count)
print("=" * 60)