"""
bronze.py
---------
Bronze layer for the nse-stock-research SDP pipeline.

Reads raw JSON files from the landing volume using Auto Loader (cloudFiles)
and writes them to bronze tables -- one per source -- with no business logic.

Sources and their landing subfolders:
  companies     -> /Volumes/<catalog>/landing/raw_landing/companies/
  prices        -> /Volumes/<catalog>/landing/raw_landing/prices/
  fundamentals  -> /Volumes/<catalog>/landing/raw_landing/fundamentals/
  news          -> /Volumes/<catalog>/landing/raw_landing/news/

Each source produces one streaming table:
  <catalog>.bronze.raw_companies
  <catalog>.bronze.raw_prices
  <catalog>.bronze.raw_fundamentals
  <catalog>.bronze.raw_news

Auto Loader handles schema inference and incremental file discovery.
New files dropped into the landing subfolders are automatically picked
up on the next pipeline refresh -- no manual intervention needed.

This file is loaded by the SDP pipeline via the glob in pipeline.yml.
Configuration values (catalog, landing_schema, landing_volume) are passed
from pipeline.yml -> configuration section -> spark.conf here.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Pipeline configuration -- read from spark.conf.
# These values come from the `configuration:` block in pipeline.yml.
CATALOG = spark.conf.get("catalog")
LANDING_SCHEMA = spark.conf.get("landing_schema", "landing")
LANDING_VOLUME = spark.conf.get("landing_volume", "raw_landing")

# Full path to the landing volume root.
# Each source writes JSON files to a subfolder under this path.
LANDING = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}"

# Source list -- one streaming table per source.
# companies/fundamentals: one JSON object per file.
# prices/news: one JSON array per file -- multiLine lets Spark explode
# the array into one row per element instead of one row per file.
SOURCES = ["companies", "prices", "fundamentals", "news"]

# Loop over each source and declare a streaming table.
# The `source=source` default argument captures the current loop value
# so each table function references the correct source at definition time.
for source in SOURCES:

    @dp.table(
        name=f"{CATALOG}.bronze.raw_{source}",
        comment=f"Raw {source} landed from ingestion, Auto Loader over the landing volume.",
    )
    def _ingest_source(source=source):
        return (
            spark.readStream.format("cloudFiles")
            # Auto Loader: incrementally discovers new JSON files.
            .option("cloudFiles.format", "json")
            # Schema location: Auto Loader stores inferred schema here
            # so it can detect schema changes across runs.
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schemas/{source}")
            # Infer column types (numbers, booleans) instead of
            # treating everything as strings.
            .option("cloudFiles.inferColumnTypes", "true")
            # multiLine: parse JSON arrays as multiple rows.
            # Without this, a file containing [{...}, {...}] would
            # be treated as a single corrupt record.
            .option("multiLine", "true")
            .load(f"{LANDING}/{source}")
            # Add metadata columns for traceability:
            # _ingested_at -- when the row was loaded into bronze.
            .withColumn("_ingested_at", F.current_timestamp())
            # _source_file -- the path of the original JSON file.
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )
