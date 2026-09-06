"""
Silver layer: typed, deduplicated tables built from bronze.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(
    name=f"{CATALOG}.silver.companies",
    comment="Company profile + latest fundamentals snapshot per ticker.",
)
def companies():
    profiles = spark.read.table(f"{CATALOG}.bronze.raw_companies")
    fundamentals = spark.read.table(f"{CATALOG}.bronze.raw_fundamentals")

    latest_fundamentals = (
        fundamentals
        .withColumn(
            "_rn",
            F.row_number().over(Window.partitionBy("ticker").orderBy(F.col("as_of").desc())),
        )
        .filter("_rn = 1")
        .drop("_rn")
    )

    return (
        profiles.alias("p")
        .join(latest_fundamentals.alias("f"), on="ticker", how="left")
        .select(
            F.col("p.ticker").alias("ticker"),
            F.col("p.company_name").alias("company_name"),
            F.col("p.sector").alias("sector"),
            F.col("p.exchange").alias("exchange"),
            F.col("p.profile").alias("profile"),
            F.col("f.marketCap").alias("market_cap"),
            F.col("f.trailingPE").alias("trailing_pe"),
            F.col("f.forwardPE").alias("forward_pe"),
            F.col("f.trailingEps").alias("trailing_eps"),
            F.col("f.dividendYield").alias("dividend_yield"),
            F.col("f.fiftyTwoWeekHigh").alias("fifty_two_week_high"),
            F.col("f.fiftyTwoWeekLow").alias("fifty_two_week_low"),
            F.col("f.currency").alias("currency"),
            F.col("f.as_of").alias("fundamentals_as_of"),
        )
    )


@dp.table(
    name=f"{CATALOG}.silver.price_snapshots",
    comment="Typed daily OHLCV price snapshots.",
)
@dp.expect("valid_ticker", "ticker IS NOT NULL")
@dp.expect("valid_date", "date IS NOT NULL")
@dp.expect_or_drop("positive_close", "close IS NOT NULL AND close > 0")
def price_snapshots():
    return (
        spark.readStream.table(f"{CATALOG}.bronze.raw_prices")
        .select(
            F.col("ticker"),
            F.to_date("date").alias("date"),
            F.col("open").cast("double").alias("open"),
            F.col("high").cast("double").alias("high"),
            F.col("low").cast("double").alias("low"),
            F.col("close").cast("double").alias("close"),
            F.col("volume").cast("long").alias("volume"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.news_articles",
    comment="Deduplicated news articles -- same article_id can land repeatedly across ingestion runs; latest occurrence wins.",
)
def news_articles():
    raw = spark.read.table(f"{CATALOG}.bronze.raw_news")
    return (
        raw
        .withColumn(
            "_rn",
            F.row_number().over(Window.partitionBy("article_id").orderBy(F.col("_ingested_at").desc())),
        )
        .filter("_rn = 1")
        .drop("_rn")
        .select("article_id", "source", "title", "link", "summary", "published", "_ingested_at")
    )

