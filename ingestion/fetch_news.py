"""
fetch_news.py
─────────────
Fetches market news articles from Indian financial RSS feeds and writes them
as JSON arrays to the landing volume.

Each run produces one JSON file per RSS source, stamped with the run timestamp.
The file contains an array of article objects (title, link, summary, published).

Output path: /Volumes/nse_stock_research/landing/raw_landing/news/<source>_<timestamp>.json

The bronze layer (bronze.py) picks up these files via Auto Loader and loads
them into the bronze.raw_news table.

Run via: DABs job task `fetch_news` (spark_python_task)
Dependencies: feedparser, requests (installed via job environment spec)
"""
import hashlib
import json
import os
from datetime import datetime, timezone
import requests

import feedparser

# Landing volume subfolder for news JSON files.
NEWS_PATH = "/Volumes/nse_stock_research/landing/raw_landing/news"

# ─────────────────────────────────────────────
# RSS feeds covering Indian market news.
# Add or remove feeds here to change news sources.
# ─────────────────────────────────────────────
FEEDS = {
    "moneycontrol": "https://www.moneycontrol.com/rss/latestnews.xml",
    "economic_times": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "livemint": "https://www.livemint.com/rss/markets",
    "business_standard": "https://www.business-standard.com/rss/markets-106.rss",
}


def fetch_feed(source: str, url: str):
    """Fetch and parse a single RSS feed.

    Returns a list of article dicts with a deterministic article_id
    (SHA-256 hash of link or title) to enable deduplication downstream.
    """
    # Use a browser User-Agent to avoid being blocked by some RSS endpoints.
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    parsed = feedparser.parse(resp.text)
    articles = []
    for entry in parsed.entries:
        # Hash the link (or title as fallback) to generate a stable article ID.
        link_or_title = entry.get("link", entry.get("title", ""))
        article_id = hashlib.sha256(link_or_title.encode()).hexdigest()[:16]
        articles.append({
            "article_id": article_id,
            "source": source,
            "title": entry.get("title"),
            "link": entry.get("link"),
            "summary": entry.get("summary", ""),
            "published": entry.get("published", ""),
        })
    return articles


def main():
    """Fetch articles from all RSS feeds and write JSON to landing volume."""
    # Create the news subfolder inside the landing volume if it doesn't exist.
    os.makedirs(NEWS_PATH, exist_ok=True)

    # Use a timestamp (not just date) so multiple runs per day don't overwrite.
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")

    for source, url in FEEDS.items():
        try:
            articles = fetch_feed(source, url)
        except Exception as e:
            # Log failure and continue — one dead feed should not abort the run.
            print(f"FAILED {source}: {e}")
            continue

        # Write all articles from this feed as a single JSON array.
        path = f"{NEWS_PATH}/{source}_{run_ts}.json"
        with open(path, "w") as f:
            json.dump(articles, f)
        print(f"wrote {len(articles)} articles -> {path}")


if __name__ == "__main__":
    main()
