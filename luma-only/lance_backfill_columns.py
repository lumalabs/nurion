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
Lance Backfill Columns using RayDP + PySpark with Lance Namespace Catalog

This script backfills columns from a source table to a target table using
Lance Spark's UPDATE COLUMNS FROM syntax. The join is performed on:
    target._rowid = source.original_row_id

UPDATE COLUMNS FROM vs ADD COLUMNS FROM:
    - ADD COLUMNS FROM: Adds NEW columns (columns must NOT exist in target)
    - UPDATE COLUMNS FROM: Updates EXISTING columns (columns must already exist in target)

For this workflow:
    1. First run prepare_nullable_schema.py to add nullable columns to target
    2. Then run this script to update the column values from source

Source table (backfill data):
    lance.lax_other_1_compacted__v2_backfill_pipeline_run_backfill_orig_res_image

Target table (to update columns in):
    lance.storyboard_other_1_image_compacted

Columns to backfill:
    - image_res_720, image_res_720_width, image_res_720_height
    - image_res_1080, image_res_1080_width, image_res_1080_height
    - image_res_2160, image_res_2160_width, image_res_2160_height
    - image_res_2160HDR, image_res_2160HDR_width, image_res_2160HDR_height

Usage:
    # Run backfill (UPDATE existing columns)
    python -m workflows.lance_backfill_columns

    # Dry run (analyze only, no write)
    python -m workflows.lance_backfill_columns --dry-run

    # Custom tables
    python -m workflows.lance_backfill_columns --target lance.my_target --source lance.my_source
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

# Table names (using Lance Namespace Catalog format: lance.table_name)
TARGET_TABLE = "lance.storyboard_other_1_image_compacted"
SOURCE_TABLE = "lance.lax_other_1_compacted__v2_backfill_pipeline_run_backfill_orig_res_image"

# S3 paths for pylance API access
SOURCE_TABLE_PATH = "s3://ai-lumalabs-datasets-ap-se-2/lance_processing/lax_other_1_compacted__v2_backfill_pipeline_run_backfill_orig_res_image.lance"
TARGET_TABLE_PATH = "s3://ai-lumalabs-datasets-ap-se-2/lance_datasets/storyboard/image_table/other_1_compacted__v2.lance"

# Columns to backfill from source to target
BACKFILL_COLUMNS = [
    "image_res_720",
    "image_res_720_width",
    "image_res_720_height",
    "image_res_1080",
    "image_res_1080_width",
    "image_res_1080_height",
    "image_res_2160",
    "image_res_2160_width",
    "image_res_2160_height",
    "image_res_2160HDR",
    "image_res_2160HDR_width",
    "image_res_2160HDR_height",
]

# Join condition: target._rowid = source.original_row_id
JOIN_TARGET_COLUMN = "_rowid"
JOIN_SOURCE_COLUMN = "original_row_id"


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
        # Spark SQL configs - optimized for large binary data
        "spark.sql.adaptive.enabled": "true",
        # DISABLE coalesce - it merges partitions into huge ones (429GB each!) causing OOM
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        # Keep small partition size for large binary columns
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
        # Source table has 207k fragments with large binary data
        # Need many partitions to avoid OOM during shuffle
        "spark.sql.shuffle.partitions": "50000",
        "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
        # Lance SQL extensions (required for UPDATE COLUMNS FROM)
        "spark.sql.extensions": "org.lance.spark.extensions.LanceSparkSessionExtensions",
        # Lance Namespace Catalog config
        "spark.sql.catalog.lance": "org.lance.spark.LanceNamespaceSparkCatalog",
        "spark.sql.catalog.lance.impl": "rest",
        "spark.sql.catalog.lance.header.x-auth-token": api_token,
        "spark.sql.catalog.lance.uri": api_uri,
        "spark.sql.catalog.lance.delimiter": "_",
        # Serialization - Kryo is more efficient for binary data
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "512m",
        "spark.local.dir": "/tmp/spark",
        # S3/Hadoop configs
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.block.size": "128M",
        "spark.hadoop.fs.s3a.multipart.size": "64M",
        "spark.hadoop.fs.s3a.connection.maximum": "200",
        "spark.hadoop.fs.s3a.threads.max": "100",
        "spark.sql.pyspark.jvmStacktrace.enabled": "true",
        # Fault tolerance - increase retries for large data transfers
        "spark.task.maxFailures": "10",
        "spark.stage.maxConsecutiveAttempts": "10",
        "spark.sql.files.ignoreCorruptFiles": "true",
        "spark.sql.files.ignoreMissingFiles": "true",
        # Network timeout settings - longer for large binary transfers
        "spark.network.timeout": "1200s",
        "spark.executor.heartbeatInterval": "60s",
        "spark.sql.broadcastTimeout": "1200",
        "spark.rpc.askTimeout": "600s",
        # Shuffle settings for large binary data
        "spark.shuffle.io.maxRetries": "10",
        "spark.shuffle.io.retryWait": "60s",
        "spark.reducer.maxReqsInFlight": "1",
        "spark.shuffle.io.backLog": "8192",
        "spark.shuffle.file.buffer": "1m",
        "spark.shuffle.spill.compress": "true",
        "spark.shuffle.compress": "true",
        # Memory management - more memory for shuffle/storage with large binary
        "spark.memory.fraction": "0.7",
        "spark.memory.storageFraction": "0.2",
        # Reduce GC pressure with large objects
        "spark.executor.extraJavaOptions": (
            "-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:InitiatingHeapOccupancyPercent=35"
        ),
        # Driver result size - needed for broadcast join with many tasks
        "spark.driver.maxResultSize": "4g",
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


def init_ray_and_spark(
    num_executors: int = 200,
    executor_memory: str = "32g",
    executor_cores: int = 8,
) -> SparkSession:
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
            log_to_driver=False,  # Disable to reduce log volume
            job_config=job_config,
        )
        logger.info(f"Connected to Ray cluster: {ray.cluster_resources()}")

    spark = raydp.init_spark(
        app_name="LanceBackfillColumns",
        num_executors=num_executors,
        executor_cores=executor_cores,
        executor_memory=executor_memory,
        configs=get_spark_configs(),
    )

    logger.info(
        f"Spark session initialized ({num_executors} executors, "
        f"{executor_cores} cores, {executor_memory} each)"
    )
    return spark


def analyze_tables(spark: SparkSession, target_table: str, source_table: str):
    """Analyze source and target tables before backfill.

    Note: With UPDATE COLUMNS FROM, incomplete coverage is OK!
    - Rows in target that don't match source: keep original values (fallback)
    - No null values are written for unmatched rows
    """
    logger.info("=" * 70)
    logger.info("Analyzing tables...")
    logger.info("=" * 70)

    # Target table info
    logger.info(f"\n📊 Target table: {target_table}")
    target_df = spark.table(target_table)
    target_count = target_df.count()
    logger.info(f"   Row count: {target_count:,}")
    logger.info("   Schema:")
    target_df.printSchema()

    # Source table info
    logger.info(f"\n📊 Source table: {source_table}")
    source_df = spark.table(source_table)
    source_count = source_df.count()
    logger.info(f"   Row count: {source_count:,}")
    logger.info("   Schema:")
    source_df.printSchema()

    # Check join coverage
    logger.info("\n📊 Join analysis:")
    logger.info(f"   Join condition: target.{JOIN_TARGET_COLUMN} = source.{JOIN_SOURCE_COLUMN}")

    # Sample join to verify
    sample_join = spark.sql(f"""
        SELECT COUNT(*) as match_count
        FROM {target_table} t
        JOIN {source_table} s ON t.{JOIN_TARGET_COLUMN} = s.{JOIN_SOURCE_COLUMN}
    """)
    match_count = sample_join.first()["match_count"]
    coverage = (match_count / target_count * 100) if target_count > 0 else 0

    logger.info(f"   Matching rows: {match_count:,}")
    logger.info(f"   Coverage: {coverage:.2f}%")

    # Check for fragments with incomplete coverage
    # Note: With UPDATE COLUMNS FROM, incomplete coverage is OK
    # - unmatched rows keep original values
    logger.info("\n📊 Fragment coverage analysis (informational only for UPDATE COLUMNS):")
    logger.info("   Checking which fragments have incomplete coverage...")

    fragment_coverage = spark.sql(f"""
        SELECT 
            shiftright(t.{JOIN_TARGET_COLUMN}, 32) as frag_id,
            COUNT(*) as target_rows,
            COUNT(s.{JOIN_SOURCE_COLUMN}) as matched_rows
        FROM {target_table} t
        LEFT JOIN {source_table} s ON t.{JOIN_TARGET_COLUMN} = s.{JOIN_SOURCE_COLUMN}
        GROUP BY shiftright(t.{JOIN_TARGET_COLUMN}, 32)
        HAVING COUNT(*) != COUNT(s.{JOIN_SOURCE_COLUMN})
        ORDER BY frag_id
        LIMIT 20
    """)

    incomplete_frags = fragment_coverage.collect()
    if incomplete_frags:
        logger.info(f"   ℹ️  Found {len(incomplete_frags)}+ fragments with incomplete coverage")
        logger.info("   With UPDATE COLUMNS FROM, unmatched rows will keep original values:")
        for row in incomplete_frags[:10]:
            missing = row["target_rows"] - row["matched_rows"]
            logger.info(
                f"      Fragment {row['frag_id']}: {row['matched_rows']}/{row['target_rows']} "
                f"rows matched ({missing} will keep original values)"
            )
        if len(incomplete_frags) > 10:
            logger.info("      ... and more")
        logger.info("")
        logger.info("   ✅ UPDATE COLUMNS FROM handles this gracefully - safe to proceed")
    else:
        logger.info("   ✅ All fragments have complete coverage")

    return {
        "target_count": target_count,
        "source_count": source_count,
        "match_count": match_count,
        "coverage": coverage,
        "has_incomplete_fragments": len(incomplete_frags) > 0,
    }


def get_fragment_ids(table_path: str) -> list[int]:
    """Get fragment IDs from Lance table using pylance API."""
    import lance

    logger.info(f"Getting fragment IDs from Lance table: {table_path}")
    ds = lance.dataset(table_path)
    fragment_ids = [frag.fragment_id for frag in ds.get_fragments()]
    logger.info(f"Found {len(fragment_ids)} fragments")
    return sorted(fragment_ids)


def backfill_columns_update_columns_from(
    spark: SparkSession,
    target_table: str,
    source_table: str,
    columns: list[str],
    source_table_path: str,
    num_batches: int = 20,
    start_batch: int = 0,
):
    """
    Backfill columns using Lance's UPDATE COLUMNS FROM syntax.

    UPDATE COLUMNS FROM updates EXISTING columns in the target table.
    This is different from ADD COLUMNS FROM which adds NEW columns.

    Key insight: NO JOIN needed!
    - source.original_row_id = target._rowid
    - For non-stable rowid tables, _rowid = _rowaddr
    - _fragid = original_row_id >> 32 (high 32 bits)

    So we can derive _rowaddr and _fragid directly from source.original_row_id,
    avoiding the expensive join and shuffle entirely!

    Batching strategy: Batch by SOURCE fragments using original_row_id range.
    - Each batch processes a range of original_row_id values
    - Filter pushdown on original_row_id for efficient reads

    Behavior:
    - Rows in target that match source: updated with source values
    - Rows in target that don't match source: keep original values (fallback)
    - Rows in source that don't exist in target: ignored
    - _rowid and _fragid are NOT modified

    Requirements:
    - Target table columns must already exist (run prepare_nullable_schema.py first)
    - The temporary view must contain _rowaddr and _fragid columns
    """
    columns_select = ", ".join([f"s.{col}" for col in columns])
    columns_list = ", ".join(columns)

    total_start_time = time.time()

    logger.info("=" * 70)
    logger.info("Method: UPDATE COLUMNS FROM (NO JOIN - derive from original_row_id)")
    logger.info("=" * 70)
    logger.info("Strategy: Batch by source fragment, derive _rowaddr/_fragid from original_row_id")
    logger.info("  - _rowaddr = original_row_id (for non-stable rowid tables)")
    logger.info("  - _fragid = shiftright(original_row_id, 32)")
    logger.info("  - NO JOIN needed - avoids expensive shuffle!")
    logger.info("  - UPDATE (not ADD) - columns must already exist in target")

    # Get SOURCE fragment IDs for batching
    source_frag_ids = get_fragment_ids(source_table_path)
    total_frags = len(source_frag_ids)
    logger.info(f"Source table has {total_frags} fragments")

    # Calculate fragments per batch
    frags_per_batch = (total_frags + num_batches - 1) // num_batches
    logger.info(f"Total batches: {num_batches}, ~{frags_per_batch} fragments per batch")
    logger.info(f"Starting from batch: {start_batch}")

    for batch_idx in range(start_batch, num_batches):
        batch_start_idx = batch_idx * frags_per_batch
        batch_end_idx = min((batch_idx + 1) * frags_per_batch, total_frags)

        if batch_start_idx >= total_frags:
            logger.info(f"Batch {batch_idx + 1}: No more fragments to process")
            break

        batch_frag_ids = source_frag_ids[batch_start_idx:batch_end_idx]
        min_frag_id = min(batch_frag_ids)
        max_frag_id = max(batch_frag_ids)

        # Calculate original_row_id range for this batch (for filter pushdown)
        min_rowid = min_frag_id << 32
        max_rowid = (max_frag_id << 32) | 0xFFFFFFFF

        logger.info("=" * 70)
        logger.info(f"Processing batch {batch_idx + 1}/{num_batches}")
        logger.info(
            f"Source fragments: {len(batch_frag_ids)} (IDs: {min_frag_id} to {max_frag_id})"
        )
        logger.info(f"original_row_id range: {min_rowid} to {max_rowid}")
        logger.info("=" * 70)

        # Create view WITHOUT joining Target table
        # Derive _rowaddr and _fragid directly from original_row_id
        create_view_sql = f"""
            CREATE OR REPLACE TEMPORARY VIEW backfill_view AS
            SELECT
                s.{JOIN_SOURCE_COLUMN} AS _rowaddr,
                CAST(shiftright(s.{JOIN_SOURCE_COLUMN}, 32) AS INT) AS _fragid,
                {columns_select}
            FROM {source_table} s
            WHERE s.{JOIN_SOURCE_COLUMN} BETWEEN {min_rowid} AND {max_rowid}
        """

        logger.info(f"Creating batch view for {len(batch_frag_ids)} source fragments...")
        spark.sql(create_view_sql)

        update_columns_sql = f"""
            ALTER TABLE {target_table}
            UPDATE COLUMNS {columns_list}
            FROM backfill_view
        """

        logger.info(f"Executing UPDATE COLUMNS FROM for batch {batch_idx + 1}...")
        batch_start_time = time.time()
        spark.sql(update_columns_sql)
        batch_elapsed = time.time() - batch_start_time

        logger.info(f"✅ Batch {batch_idx + 1}/{num_batches} completed in {batch_elapsed:.2f}s")

        # Estimate remaining time
        batches_done = batch_idx - start_batch + 1
        batches_remaining = num_batches - batch_idx - 1
        if batches_remaining > 0:
            avg_time = (time.time() - total_start_time) / batches_done
            eta = avg_time * batches_remaining
            logger.info(f"ETA for remaining {batches_remaining} batches: {eta / 60:.1f} minutes")

    total_elapsed = time.time() - total_start_time
    logger.info("=" * 70)
    logger.info(
        f"✅ All batches completed in {total_elapsed:.2f}s ({total_elapsed / 60:.1f} minutes)"
    )
    logger.info("=" * 70)

    # Verify result
    logger.info("\nVerifying result...")
    result_df = spark.table(target_table)
    result_df.printSchema()

    # Sample check
    sample = spark.sql(f"""
        SELECT {columns_list}
        FROM {target_table}
        WHERE {columns[0]} IS NOT NULL
        LIMIT 5
    """)
    logger.info("Sample rows with new columns:")
    sample.show(truncate=False)


def backfill_columns_fragment_aware_join(
    spark: SparkSession,
    target_table: str,
    source_table: str,
    columns: list[str],
    output_table: str,
):
    """
    Backfill columns using Fragment-Aware Join optimization.

    This method creates a new table with all columns merged.
    Uses Lance Spark's automatic Fragment-Aware Join optimization
    when joining on _rowid/_rowaddr columns.
    """
    logger.info("=" * 70)
    logger.info("Method: Fragment-Aware Join (creates new table)")
    logger.info("=" * 70)

    # Build column selection
    # Get all columns from target table
    target_df = spark.table(target_table)
    target_columns = [f"t.{col}" for col in target_df.columns if not col.startswith("_")]

    # Add new columns from source
    source_columns = [f"s.{col}" for col in columns]

    all_columns = ", ".join(target_columns + source_columns)

    # Use FRAGMENT_AWARE_JOIN hint for optimization
    join_sql = f"""
        SELECT /*+ FRAGMENT_AWARE_JOIN(s) */
            {all_columns}
        FROM {target_table} t
        JOIN {source_table} s ON t.{JOIN_TARGET_COLUMN} = s.{JOIN_SOURCE_COLUMN}
    """

    logger.info("Executing Fragment-Aware Join...")
    logger.info(f"SQL:\n{join_sql}")

    joined_df = spark.sql(join_sql)

    # Check query plan for optimization
    logger.info("\nQuery plan (checking for Fragment-Aware optimization):")
    plan_str = joined_df._jdf.queryExecution().optimizedPlan().toString()
    if "RepartitionByExpression" in plan_str or "_lance_frag_id" in plan_str:
        logger.info("✅ Fragment-Aware Join optimization is active!")
    else:
        logger.warning("⚠️ Fragment-Aware Join optimization may not be applied")

    # Write to output table
    logger.info(f"\nWriting to output table: {output_table}")
    start_time = time.time()

    # Create temp view and INSERT INTO
    joined_df.createOrReplaceTempView("joined_result")
    spark.sql(f"INSERT OVERWRITE {output_table} SELECT * FROM joined_result")

    elapsed = time.time() - start_time
    logger.info(f"✅ Write completed in {elapsed:.2f}s")

    # Verify result
    result_count = spark.table(output_table).count()
    logger.info(f"Output table row count: {result_count:,}")


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
    parser = argparse.ArgumentParser(
        description="Backfill columns from source Lance table to target table"
    )
    parser.add_argument(
        "--target",
        default=TARGET_TABLE,
        help=f"Target Lance table name (default: {TARGET_TABLE})",
    )
    parser.add_argument(
        "--source",
        default=SOURCE_TABLE,
        help=f"Source Lance table name (default: {SOURCE_TABLE})",
    )
    parser.add_argument(
        "--source-path",
        default=SOURCE_TABLE_PATH,
        help=f"Source Lance table S3 path for pylance API (default: {SOURCE_TABLE_PATH})",
    )
    parser.add_argument(
        "--columns",
        nargs="+",
        default=BACKFILL_COLUMNS,
        help=f"Columns to backfill (default: {BACKFILL_COLUMNS})",
    )
    parser.add_argument(
        "--method",
        choices=["update-columns", "join"],
        default="update-columns",
        help=(
            "Backfill method: 'update-columns' (Spark UPDATE COLUMNS FROM) "
            "or 'join' (Spark, new table)"
        ),
    )
    parser.add_argument(
        "--output-table",
        default=None,
        help="Output table name (only for 'join' method)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only analyze tables, don't perform backfill",
    )
    parser.add_argument(
        "--num-executors",
        type=int,
        default=200,
        help="Number of Spark executors (default: 200)",
    )
    parser.add_argument(
        "--executor-memory",
        default="32g",
        help="Memory per executor (default: 32g)",
    )
    parser.add_argument(
        "--executor-cores",
        type=int,
        default=8,
        help="Cores per executor (default: 8)",
    )
    parser.add_argument(
        "--num-batches",
        type=int,
        default=20,
        help="Number of batches for add-columns method (default: 20)",
    )
    parser.add_argument(
        "--start-batch",
        type=int,
        default=0,
        help="Start from batch N (0-indexed, for resuming failed jobs)",
    )
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("Lance Backfill Columns (Lance Namespace Catalog)")
    logger.info("=" * 70)
    logger.info(f"API URI: {API_URI}")
    logger.info(f"Target table: {args.target}")
    logger.info(f"Source table: {args.source}")
    logger.info(f"Columns to backfill: {args.columns}")
    logger.info(f"Method: {args.method}")
    logger.info(f"Dry run: {args.dry_run}")

    try:
        spark = init_ray_and_spark(
            num_executors=args.num_executors,
            executor_memory=args.executor_memory,
            executor_cores=args.executor_cores,
        )

        # Analyze tables

        if args.dry_run:
            stats = analyze_tables(spark, args.target, args.source)
            logger.info("\n" + "=" * 70)
            logger.info("DRY RUN - Analysis complete, no changes made")
            logger.info("=" * 70)
            logger.info(f"Target rows: {stats['target_count']:,}")
            logger.info(f"Source rows: {stats['source_count']:,}")
            logger.info(f"Matching rows: {stats['match_count']:,}")
            logger.info(f"Coverage: {stats['coverage']:.2f}%")
            logger.info(f"Columns to add: {args.columns}")
        else:
            if args.method == "update-columns":
                backfill_columns_update_columns_from(
                    spark,
                    args.target,
                    args.source,
                    args.columns,
                    source_table_path=args.source_path,
                    num_batches=args.num_batches,
                    start_batch=args.start_batch,
                )
            elif args.method == "join":
                output_table = args.output_table or f"{args.target}_with_backfill"
                backfill_columns_fragment_aware_join(
                    spark,
                    args.target,
                    args.source,
                    args.columns,
                    output_table,
                )

        logger.info("\n" + "=" * 70)
        logger.info("✅ Backfill completed successfully!")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"❌ Backfill failed: {e}")
        raise
    finally:
        cleanup()


if __name__ == "__main__":
    main()
