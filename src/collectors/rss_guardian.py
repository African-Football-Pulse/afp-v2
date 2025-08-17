import os, json, datetime, feedparser
from urllib.parse import urlparse
from src.storage.azure_blob import put_text, utc_now_iso

ACCOUNT_CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

LEAGUE = "premier_league"
FEED_NAME = "guardian_football"
FEED_URL  = "https://www.theguardian.com/football/rss"

def normalize(entry):
    # Minimalt normaliserad post
    return {
        "title": entry.get("title", "").strip(),
        "link": entry.get("link"),
        "published": entry.get("published", ""),
        "summary": entry.get("summary", ""),
        "source": "The Guardian",
    }

def blob_date():
    # yyyy-mm-dd (UTC)
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

def main():
    d = feedparser.parse(FEED_URL)
    items = [normalize(e) for e in d.entries[:50]]

    base_raw = f"raw/news/{FEED_NAME}/{blob_date()}/"
    base_cur = f"curated/news/{FEED_NAME}/{LEAGUE}/{blob_date()}/"

    put_text(ACCOUNT_CONTAINER, base_raw + "rss.json",
             json.dumps({"feed": FEED_URL, "fetched_at": utc_now_iso(), "items": items},
                        ensure_ascii=False, indent=2),
             "application/json")

    # Minimal filtrering (behåll alla – vi kan förfina senare med keywords)
    curated = {
        "league": LEAGUE,
        "feed": FEED_URL,
        "fetched_at": utc_now_iso(),
        "items": items
    }
    put_text(ACCOUNT_CONTAINER, base_cur + "items.json",
             json.dumps(curated, ensure_ascii=False, indent=2),
             "application/json")

    put_text(ACCOUNT_CONTAINER, base_cur + "input_manifest.json",
             json.dumps({"source": FEED_NAME, "count": len(items), "generated_at": utc_now_iso()},
                        ensure_ascii=False, indent=2),
             "application/json")

if __name__ == "__main__":
    main()
