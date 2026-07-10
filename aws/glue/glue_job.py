"""
AWS Glue structured-file ETL job.

Flow:
    S3 raw structured file
        -> PySpark cleaning and validation
        -> Apache Iceberg table on Amazon S3
        -> AWS Glue Data Catalog registration

The full structured dataset is no longer written to PostgreSQL.
PostgreSQL remains responsible only for application and processing metadata.
"""

import re
import sys
from typing import Iterable

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    sum as spark_sum,
    trim,
    when,
)
from pyspark.sql.types import IntegerType, StringType


# ---------------------------------------------------------------------------
# Glue job arguments
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_INPUT_PATH",
        "TABLE_NAME",
        "USER_ID",
        "DOCUMENT_ID",
        "FILE_NAME",
        "ICEBERG_DATABASE",
        "ICEBERG_WAREHOUSE",
    ],
)

job_name = args["JOB_NAME"]
s3_input_path = args["S3_INPUT_PATH"]
requested_table_name = args["TABLE_NAME"]
user_id = int(args["USER_ID"])
document_id = int(args["DOCUMENT_ID"])
file_name = args["FILE_NAME"]
iceberg_database = args["ICEBERG_DATABASE"]
iceberg_warehouse = args["ICEBERG_WAREHOUSE"].rstrip("/") + "/"


# ---------------------------------------------------------------------------
# Spark and Glue initialization
# ---------------------------------------------------------------------------

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(job_name, args)

spark.conf.set("spark.sql.shuffle.partitions", "10")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# These settings are also passed through the Glue job's --conf parameter.
# Setting the catalog values here provides an additional validation/fallback.
spark.conf.set(
    "spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog",
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.warehouse",
    iceberg_warehouse,
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog",
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO",
)


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------

def clean_identifier(value: str, default: str) -> str:
    """
    Convert a value into a lowercase Spark/Glue-compatible identifier.
    """
    cleaned = str(value).strip().lower()
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")

    if not cleaned:
        cleaned = default

    # A table or database identifier should not begin with a number.
    if cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"

    return cleaned


def make_unique_column_names(columns: Iterable[str]) -> list[str]:
    """
    Clean column names and prevent collisions.

    Example:
        "Employee Name" -> employee_name
        "Employee-Name" -> employee_name_2
    """
    seen: dict[str, int] = {}
    cleaned_columns: list[str] = []

    for index, original_name in enumerate(columns, start=1):
        base_name = clean_identifier(
            original_name,
            default=f"column_{index}",
        )

        count = seen.get(base_name, 0) + 1
        seen[base_name] = count

        final_name = (
            base_name
            if count == 1
            else f"{base_name}_{count}"
        )

        cleaned_columns.append(final_name)

    return cleaned_columns


iceberg_database = clean_identifier(
    iceberg_database,
    default="chatbot_lakehouse",
)

table_name = clean_identifier(
    requested_table_name,
    default=f"u{user_id}_d{document_id}_dataset",
)

table_identifier = (
    f"glue_catalog.{iceberg_database}.{table_name}"
)


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------

def get_file_extension(path: str) -> str:
    """
    Extract the extension from an S3 path while ignoring query parameters.
    """
    clean_path = path.split("?", maxsplit=1)[0]
    file_part = clean_path.rsplit("/", maxsplit=1)[-1]

    if "." not in file_part:
        return ""

    return file_part.rsplit(".", maxsplit=1)[-1].lower()


def read_structured_file(path: str) -> DataFrame:
    """
    Load supported structured file types into a Spark DataFrame.

    Excel files are converted to CSV by FastAPI before reaching this job.
    """
    extension = get_file_extension(path)

    print(f"[glue] Detected extension: {extension}")

    if extension == "csv":
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .option("emptyValue", None)
            .csv(path)
        )

    if extension == "json":
        return (
            spark.read
            .option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .json(path)
        )

    if extension == "parquet":
        return spark.read.parquet(path)

    raise ValueError(
        "Unsupported structured file type: "
        f"'{extension or 'unknown'}'. "
        "Supported Glue inputs are CSV, JSON, and Parquet. "
        "Excel files must be converted to CSV before processing."
    )


# ---------------------------------------------------------------------------
# Cleaning and validation
# ---------------------------------------------------------------------------

def clean_dataframe(input_df: DataFrame) -> DataFrame:
    """
    Standardize column names, trim strings, remove empty and duplicate rows.
    """
    if not input_df.columns:
        raise ValueError(
            f"File '{file_name}' does not contain any readable columns."
        )

    new_column_names = make_unique_column_names(input_df.columns)

    df = input_df.toDF(*new_column_names)

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                trim(col(field.name)),
            )

    df = df.dropna(how="all")
    df = df.dropDuplicates()

    return df


def validate_data_quality(
    df: DataFrame,
    source_file_name: str,
) -> tuple[DataFrame, int]:
    """
    Reject empty datasets and print warnings for columns that are over 90% null.
    """
    df.persist(StorageLevel.MEMORY_AND_DISK)

    total_rows = df.count()

    if total_rows == 0:
        df.unpersist()

        raise ValueError(
            f"File '{source_file_name}' has zero rows after cleaning."
        )

    null_expressions = [
        spark_sum(
            when(col(field.name).isNull(), 1).otherwise(0)
        ).alias(field.name)
        for field in df.schema.fields
    ]

    if null_expressions:
        null_counts_row = df.agg(*null_expressions).first()

        for field in df.schema.fields:
            null_count = null_counts_row[field.name] or 0
            null_percentage = null_count / total_rows

            if null_percentage > 0.90:
                print(
                    "[WARNING] "
                    f"Column '{field.name}' is "
                    f"{null_percentage * 100:.0f}% null."
                )

    print(
        f"[glue] Data quality validation passed: "
        f"{total_rows} rows."
    )

    return df, total_rows


# ---------------------------------------------------------------------------
# Iceberg write
# ---------------------------------------------------------------------------

def write_to_iceberg(
    df: DataFrame,
    identifier: str,
) -> None:
    """
    Create or atomically replace one Iceberg table for the uploaded dataset.

    Each uploaded document has its own unique table name:
        u{user_id}_d{document_id}_{dataset_name}
    """
    print(f"[glue] Writing Iceberg table: {identifier}")

    (
        df.writeTo(identifier)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty(
            "write.parquet.compression-codec",
            "snappy",
        )
        .createOrReplace()
    )

    print(f"[glue] Iceberg write completed: {identifier}")


# ---------------------------------------------------------------------------
# Main job
# ---------------------------------------------------------------------------

try:
    print("=" * 70)
    print("[glue] Starting structured Lakehouse ETL")
    print(f"[glue] Job name: {job_name}")
    print(f"[glue] Input path: {s3_input_path}")
    print(f"[glue] User ID: {user_id}")
    print(f"[glue] Document ID: {document_id}")
    print(f"[glue] Source file: {file_name}")
    print(f"[glue] Iceberg database: {iceberg_database}")
    print(f"[glue] Iceberg table: {table_name}")
    print(f"[glue] Iceberg warehouse: {iceberg_warehouse}")
    print(f"[glue] Full table identifier: {table_identifier}")
    print("=" * 70)

    source_df = read_structured_file(s3_input_path)

    cleaned_df = clean_dataframe(source_df)

    cleaned_df, source_row_count = validate_data_quality(
        cleaned_df,
        file_name,
    )

    final_df = (
        cleaned_df
        .withColumn(
            "user_id",
            lit(user_id).cast(IntegerType()),
        )
        .withColumn(
            "document_id",
            lit(document_id).cast(IntegerType()),
        )
        .withColumn(
            "source_file_name",
            lit(file_name),
        )
        .withColumn(
            "processed_at",
            current_timestamp(),
        )
    )

    final_column_count = len(final_df.columns)

    print(f"[glue] Source rows: {source_row_count}")
    print(f"[glue] Final column count: {final_column_count}")
    print(f"[glue] Final columns: {final_df.columns}")
    print("[glue] Final schema:")

    final_df.printSchema()

    write_to_iceberg(
        final_df,
        table_identifier,
    )

    # Verify that Spark can read the table through Glue Catalog.
    verification_count = spark.table(table_identifier).count()

    if verification_count != source_row_count:
        raise RuntimeError(
            "Iceberg verification failed. "
            f"Expected {source_row_count} rows but found "
            f"{verification_count} rows."
        )

    print("=" * 70)
    print("[glue] Structured Lakehouse ETL completed successfully")
    print(f"[glue] Input: {s3_input_path}")
    print(f"[glue] Catalog table: {table_identifier}")
    print(f"[glue] Rows written: {verification_count}")
    print(f"[glue] Columns written: {final_column_count}")
    print("=" * 70)

    cleaned_df.unpersist()
    job.commit()

except Exception as error:
    print("=" * 70)
    print("[glue] Structured Lakehouse ETL failed")
    print(f"[glue] Error type: {type(error).__name__}")
    print(f"[glue] Error: {error}")
    print("=" * 70)

    raise