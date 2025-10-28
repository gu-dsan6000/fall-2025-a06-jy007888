import argparse, os, shutil, glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand, format_number

def write_single_csv(df, final_path):
    """
    Write a DataFrame as a single CSV file with header to an exact path.
    """
    temp_dir = final_path + "_tmpdir"
    # overwrite temp dir, coalesce to 1 file, include header
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", True)
       .csv(temp_dir))

    # find the single part-*.csv file Spark produced
    part_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
    if not part_files:
        raise RuntimeError(f"No part file found in {temp_dir}")
    part_file = part_files[0]

    # ensure output directory exists
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    # move/rename the part file to the exact requested filename
    shutil.move(part_file, final_path)
    # clean up the temp dir
    shutil.rmtree(temp_dir, ignore_errors=True)

def main():
    ap = argparse.ArgumentParser(description="Problem 1: Log Level Distribution")
    ap.add_argument("master", nargs="?", help="Spark master URL (e.g., spark://10.0.0.1:7077)")
    ap.add_argument("--net-id", required=True, help="Your net ID (used to build default S3 path)")
    ap.add_argument("--src", default=None, help="Override input path; default uses data/sample locally or S3 on cluster")
    args = ap.parse_args()

    outdir = "data/output"
    os.makedirs(outdir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName(f"problem1-{args.net_id}")
        .master(args.master if args.master else "local[*]")
        .getOrCreate()
    )

    
    src = args.src or (f"s3://{args.net_id}-assignment-spark-cluster-logs/data/**"
                       if args.master else "data/sample/**")

    # Read 
    df = spark.read.text(src).toDF("value")

    # keep original line as log_entry
    parsed = df.select(
        regexp_extract("value", r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias("timestamp"),
        regexp_extract("value", r'(INFO|WARN|ERROR|DEBUG)', 1).alias("log_level"),
        col("value").alias("log_entry"),
    )

    # Keep only rows with a recognized log level
    with_levels = parsed.filter(col("log_level") != "")

    # Counts per log level
    counts = with_levels.groupBy("log_level").count()

    # Sample: 10 random log entries with their levels
    sample = with_levels.orderBy(rand()).limit(10).select("log_entry", "log_level")

    # Summary text
    total_lines = df.count()
    total_with_levels = with_levels.count()
    unique_levels = counts.count()
    dist = counts.select(
        "log_level",
        "count",
        format_number(col("count") * 100.0 / (total_with_levels if total_with_levels else 1), 2).alias("pct")
    )

    summary_lines = [
        f"Total log lines processed: {total_lines}",
        f"Total lines with log levels: {total_with_levels}",
        f"Unique log levels found: {unique_levels}",
        "",
        "Log level distribution:"
    ]
    for r in dist.orderBy(col("log_level")).collect():
        lvl = f"{r['log_level']:<5}"
        summary_lines.append(f"  {lvl}: {int(r['count']):>10} ({r['pct']}%)")

    write_single_csv(counts.orderBy(col("count").desc()), os.path.join(outdir, "problem1_counts.csv"))
    write_single_csv(sample, os.path.join(outdir, "problem1_sample.csv"))
    with open(os.path.join(outdir, "problem1_summary.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))

    spark.stop()

if __name__ == "__main__":
    main()