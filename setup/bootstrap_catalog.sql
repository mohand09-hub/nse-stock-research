-- One-time setup. Run once in SQL Editor before deploying the bundle.

CREATE CATALOG IF NOT EXISTS nse_stock_research
  COMMENT 'AI Stock Market Research Assistant: NSE/BSE prices, fundamentals, and news';

CREATE SCHEMA IF NOT EXISTS nse_stock_research.landing;
CREATE SCHEMA IF NOT EXISTS nse_stock_research.bronze;
CREATE SCHEMA IF NOT EXISTS nse_stock_research.silver;
CREATE SCHEMA IF NOT EXISTS nse_stock_research.gold;

-- ingestion/fetch_prices.py and ingestion/fetch_news.py (Phase 3) write raw JSON here.
-- Auto Loader (Phase 4 bronze.py) reads from its subfolders — created automatically
-- on first file write, nothing else to do here.
CREATE VOLUME IF NOT EXISTS nse_stock_research.landing.raw_landing
  COMMENT 'Landing zone for raw yfinance price snapshots and RSS news JSON';
