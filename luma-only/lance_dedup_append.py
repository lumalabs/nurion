# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Lance Dedup and Append using RayDP + PySpark with Lance Namespace Catalog

This script demonstrates how to:
1. Read data from a Lance table using Lance Namespace Catalog (table names)
2. Deduplicate by (original_video_path, frame_index)
3. Append the deduplicated data to another Lance table

Uses Lance Namespace REST API for table management instead of S3 paths.

Source: lance.testv1
Target: lance.video_keyframe__v1

Usage:
    # Run with default tables
    python -m workflows.lance_dedup_append

    # Dry run (no write)
    python -m workflows.lance_dedup_append --dry-run
"""

import argparse
import logging
import os
import time

import ray
import raydp
from pyspark.sql import SparkSession
from ray.job_config import JobConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Lance Namespace API configuration
API_URI = "https://b0b7c37fc317-data-api-staging.sydney3.labs.lumalabs.ai/api/v1/lance-namespace"
API_TOKEN = "6c2c3ca8-43f3-4230-8620-f3f67f6133f2"

# Table names (using Lance Namespace Catalog format: lance.namespace.table)
SOURCE_TABLE = "lance.testv1"
TARGET_TABLE = "lance.video_keyframe__v1"

# Dedup columns
DEDUP_COLUMNS = ["original_video_path", "frame_index"]


def get_spark_configs(
    api_uri: str = API_URI,
    api_token: str = API_TOKEN,
    aws_access_key: str | None = None,
    aws_secret_key: str | None = None,
) -> dict[str, str]:
    """Get Spark configurations for Lance Namespace Catalog and S3."""
    aws_access_key = aws_access_key or os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = aws_secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_region = os.environ.get("AWS_REGION") or os.environ.get(
        "AWS_DEFAULT_REGION", "ap-southeast-2"
    )

    configs = {
        # Class loading priority
        "spark.driver.userClassPathFirst": "true",
        "spark.executor.userClassPathFirst": "true",
        # Spark SQL configs
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
        "spark.sql.shuffle.partitions": "2000",
        "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
        # Lance SQL extensions
        "spark.sql.extensions": "org.lance.spark.extensions.LanceSparkSessionExtensions",
        # Lance Namespace Catalog config
        "spark.sql.catalog.lance": "org.lance.spark.LanceNamespaceSparkCatalog",
        "spark.sql.catalog.lance.impl": "rest",
        "spark.sql.catalog.lance.header.x-auth-token": api_token,
        "spark.sql.catalog.lance.uri": api_uri,
        "spark.sql.catalog.lance.delimiter": "_",
        # Serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.local.dir": "/tmp/spark",
        # S3/Hadoop configs
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.block.size": "128M",
        "spark.hadoop.fs.s3a.multipart.size": "64M",
        "spark.sql.pyspark.jvmStacktrace.enabled": "true",
    }

    if aws_access_key and aws_secret_key:
        configs.update(
            {
                "spark.hadoop.fs.s3a.access.key": aws_access_key,
                "spark.hadoop.fs.s3a.secret.key": aws_secret_key,
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                ),
            }
        )
    else:
        configs["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )

    if aws_region:
        configs["spark.hadoop.fs.s3a.endpoint.region"] = aws_region

    return configs


def init_ray_and_spark(num_executors: int = 50, executor_memory: str = "32g") -> SparkSession:
    """Initialize Ray and create Spark session via RayDP."""
    logger.info("Initializing Spark session via RayDP...")

    if not ray.is_initialized():
        ray_address = os.environ.get("RAY_ADDRESS", "auto")
        logger.info(f"Connecting to Ray cluster at: {ray_address}")

        jars_paths = raydp.code_search_path()
        logger.info(f"Java code search paths: {jars_paths}")

        job_config = JobConfig(code_search_path=jars_paths)
        ray.init(
            address=ray_address,
            log_to_driver=True,
            job_config=job_config,
        )
        logger.info(f"Connected to Ray cluster: {ray.cluster_resources()}")

    spark = raydp.init_spark(
        app_name="LanceDedupAppend",
        num_executors=num_executors,
        executor_cores=8,
        executor_memory=executor_memory,
        configs=get_spark_configs(),
        log_to_driver=True,
    )

    logger.info(f"Spark session initialized ({num_executors} executors, {executor_memory} each)")
    return spark


def read_lance_table(spark: SparkSession, table_name: str):
    """Read a Lance table into a Spark DataFrame."""
    logger.info(f"Reading Lance table: {table_name}")
    df = spark.table(table_name)
    row_count = df.count()
    logger.info(f"Loaded {row_count} rows from Lance table")
    return df


def write_lance_table(spark: SparkSession, df, table_name: str, mode: str = "append"):
    """Write a Spark DataFrame to a Lance table using INSERT INTO."""
    logger.info(f"Writing to Lance table: {table_name} (mode={mode})")

    # Create temp view and use INSERT INTO
    temp_view = f"temp_dedup_{int(time.time())}"
    df.createOrReplaceTempView(temp_view)

    if mode == "append":
        sql = f"INSERT INTO {table_name} SELECT * FROM {temp_view}"
    elif mode == "overwrite":
        sql = f"INSERT OVERWRITE {table_name} SELECT * FROM {temp_view}"
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    logger.info(f"Executing: {sql}")
    spark.sql(sql)
    logger.info(f"Successfully wrote data to Lance table: {table_name}")


def dedup_and_append(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    dedup_columns: list,
):
    """Read source, deduplicate, and append to target."""
    start_time = time.time()

    # Step 1: Read source data
    logger.info("=" * 60)
    logger.info("Step 1: Reading source data")
    logger.info("=" * 60)
    source_df = read_lance_table(spark, source_table)
    source_count = source_df.count()
    logger.info(f"Source row count: {source_count}")
    source_df.printSchema()

    # Step 2: Deduplicate
    logger.info("=" * 60)
    logger.info(f"Step 2: Deduplicating by {dedup_columns}")
    logger.info("=" * 60)
    deduped_df = source_df.dropDuplicates(dedup_columns)
    deduped_count = deduped_df.count()
    duplicates_removed = source_count - deduped_count
    logger.info(f"After dedup: {deduped_count} rows")
    logger.info(f"Duplicates removed: {duplicates_removed}")

    # Step 3: Append to target
    logger.info("=" * 60)
    logger.info("Step 3: Appending to target table")
    logger.info("=" * 60)
    write_lance_table(spark, deduped_df, target_table, mode="append")

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info(f"Source rows: {source_count}")
    logger.info(f"After dedup: {deduped_count}")
    logger.info(f"Duplicates removed: {duplicates_removed}")
    logger.info(f"Total time: {elapsed:.2f}s")


def cleanup():
    """Cleanup Ray and Spark resources."""
    logger.info("Cleaning up resources...")
    try:
        raydp.stop_spark()
        logger.info("RayDP/Spark session stopped")
    except Exception as e:
        logger.warning(f"Error stopping RayDP/Spark session: {e}")

    time.sleep(2)

    try:
        ray.shutdown()
        logger.info("Ray shutdown complete")
    except Exception as e:
        logger.warning(f"Error shutting down Ray: {e}")


def main():
    parser = argparse.ArgumentParser(description="Deduplicate Lance table and append to target")
    parser.add_argument(
        "--source",
        default=SOURCE_TABLE,
        help=f"Source Lance table name (default: {SOURCE_TABLE})",
    )
    parser.add_argument(
        "--target",
        default=TARGET_TABLE,
        help=f"Target Lance table name (default: {TARGET_TABLE})",
    )
    parser.add_argument(
        "--dedup-columns",
        nargs="+",
        default=DEDUP_COLUMNS,
        help=f"Columns for deduplication (default: {DEDUP_COLUMNS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only read and dedup, don't write to target",
    )
    parser.add_argument(
        "--num-executors",
        type=int,
        default=50,
        help="Number of Spark executors (default: 50)",
    )
    parser.add_argument(
        "--executor-memory",
        default="32g",
        help="Memory per executor (default: 32g)",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Lance Dedup and Append (Lance Namespace Catalog)")
    logger.info("=" * 60)
    logger.info(f"API URI: {API_URI}")
    logger.info(f"Source: {args.source}")
    logger.info(f"Target: {args.target}")
    logger.info(f"Dedup columns: {args.dedup_columns}")
    logger.info(f"Dry run: {args.dry_run}")

    try:
        spark = init_ray_and_spark(
            num_executors=args.num_executors,
            executor_memory=args.executor_memory,
        )

        if args.dry_run:
            logger.info("DRY RUN - Reading and deduplicating only")
            source_df = read_lance_table(spark, args.source)
            source_count = source_df.count()
            deduped_df = source_df.dropDuplicates(args.dedup_columns)
            deduped_count = deduped_df.count()

            logger.info(f"Source rows: {source_count}")
            logger.info(f"After dedup: {deduped_count}")
            logger.info(f"Would remove: {source_count - deduped_count} duplicates")
        else:
            dedup_and_append(
                spark,
                args.source,
                args.target,
                args.dedup_columns,
            )

    finally:
        cleanup()

    logger.info("Done!")


if __name__ == "__main__":
    main()
