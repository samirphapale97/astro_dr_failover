import argparse
import logging
import time
from dataclasses import dataclass
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dr_deep_clone")

spark = SparkSession.builder.getOrCreate()

# Parse Arguments
parser = argparse.ArgumentParser(description="DR Deep Clone - Incremental table replication")
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--source_table", required=True)
parser.add_argument("--target_table", required=True)
args = parser.parse_args()


@dataclass
class CloneMetrics:
    source_rows: int = 0
    source_bytes: int = 0
    dest_rows: int = 0
    dest_bytes: int = 0
    dest_files: int = 0
    elapsed_sec: float = 0.0

    @property
    def throughput_gb_min(self):
        if self.elapsed_sec > 0:
            return (self.source_bytes / (1024 ** 3)) / (self.elapsed_sec / 60)
        return 0.0

    @property
    def rows_match(self):
        return self.source_rows == self.dest_rows

    def _fmt_size(self, b):
        for unit in ["bytes", "KB", "MB", "GB", "TB"]:
            if b < 1024:
                return f"{b:.2f} {unit}"
            b /= 1024

    def summary(self):
        return (
            f"rows={self.dest_rows:,}  size={self._fmt_size(self.dest_bytes)}  "
            f"files={self.dest_files:,}  time={self.elapsed_sec:.1f}s  "
            f"throughput={self.throughput_gb_min:.2f} GB/min  "
            f"validation={'PASS' if self.rows_match else 'FAIL'}"
        )


def _sanitize_name(table_input):
    """Wraps each part of a 3-part table name in backticks."""
    parts = [p.strip("`") for p in table_input.split(".")]
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.schema.table, got: {table_input}")
    return parts, ".".join(f"`{p}`" for p in parts)


def bootstrap_target(target_input):
    """Creates target catalog and schema if they don't exist."""
    parts, fq_name = _sanitize_name(target_input)
    catalog, schema, _ = parts

    logger.info("Bootstrapping target: catalog=%s schema=%s", catalog, schema)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    return fq_name


def collect_metrics(table_name):
    """Returns (row_count, size_bytes, num_files) for a Delta table."""
    row_count = spark.read.table(table_name).count()
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    return row_count, detail["sizeInBytes"], detail["numFiles"]


def run_clone(source, target):
    """Executes incremental deep clone with metrics and validation."""
    metrics = CloneMetrics()

    # Bootstrap
    fq_target = bootstrap_target(target)
    _, fq_source = _sanitize_name(source)

    # Source metrics
    metrics.source_rows, metrics.source_bytes, _ = collect_metrics(source)
    logger.info("Source: %s (%s, %s rows)", source, metrics._fmt_size(metrics.source_bytes), f"{metrics.source_rows:,}")

    # Clone
    logger.info("Executing incremental deep clone: %s -> %s", fq_source, fq_target)
    start = time.time()
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fq_target} DEEP CLONE {fq_source}")
    metrics.elapsed_sec = time.time() - start

    # Dest metrics
    metrics.dest_rows, metrics.dest_bytes, metrics.dest_files = collect_metrics(target)

    # Validation
    if not metrics.rows_match:
        logger.warning("Row count mismatch: source=%s dest=%s", f"{metrics.source_rows:,}", f"{metrics.dest_rows:,}")

    logger.info("Clone complete: %s", metrics.summary())
    return metrics


# ── Main ──
try:
    logger.info("Starting DR clone: %s -> %s", args.source_table, args.target_table)
    result = run_clone(args.source_table, args.target_table)

    if not result.rows_match:
        raise RuntimeError(
            f"Row validation failed: source={result.source_rows:,} dest={result.dest_rows:,}"
        )

except Exception:
    logger.exception("Clone failed")
    raise
