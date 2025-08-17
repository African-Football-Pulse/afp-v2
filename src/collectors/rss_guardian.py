import os
import json
import datetime
import requests
import feedparser

from src.storage.azure_blob import put_text, utc_now_iso

# ---- Config via env (minimerad hårdkodning) -------------------------------
CONTAINER   = os.getenv("AZURE_CONTAINER", "afp")
LEAGUE      = os.getenv("LEAGUE", "premier_league")
FEED_NAME   = os.getenv("FEED_NAME", "guardian_football")
FEED_URL    = os.getenv("FEED_URL", "https://www.theguardian.com/football/rss")
SOURCE_NAME = os.getenv("SOURCE_NAME", "The Guardian")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_S", "15"))  # sekunder

# ---------------------------------------------------------------------------
def _today() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

def _raw_base() -> str:
    return f"raw/news/{FEED_NAME}/{_today()}/"

def _curated_base() -> str:
    return f"curated/news/{FEED_NAME}/{LEAGUE}/{_today()}/"

def _normalize(entry):
    return {
        "title": entry.get("title", "").strip(),
        "link": entry.get("link"),
        "published": entry.get("published", ""),
        "summary": entry.get("summary", ""),
        "source": SOURCE_NAME,
    }

def main():
    print(f"🚀 [rss] start league={LEAGUE} feed={FEED_NAME} url={FEED_URL}")

    # 1) Hämta RSS med tydlig timeout
    try:
        print(f"📡 [rss] fetching… timeout={REQUEST_TIMEOUT}s")
        resp = requests.get(FEED_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        msg = {"status": "timeout", "url": FEED_URL, "timeout_s": REQUEST_TIMEOUT, "ts": utc_now_iso()}
        print(f"⏰ [rss] timeout: {msg}")
        put_text(CONTAINER, _raw_base() + "timeout.json", json.dumps(msg, ensure_ascii=False, indent=2), "application/json")
        return
    except Exception as e:
        msg = {"status": "error", "url": FEED_URL, "error": str(e), "ts": utc_now_iso()}
        print(f"❌ [rss] fetch error: {msg}")
        put_text(CONTAINER, _raw_base() + "error.json", json.dumps(msg, ensure_ascii=False, indent=2), "application/json")
        return

    # 2) Parsea och normalisera
    feed = feedparser.parse(resp.content)
    items = [_normalize(e) for e in feed.entries]
    count = len(items)
    print(f"✅ [rss] fetched items={count}")

    # 3) Skriv RAW-dump
    raw_payload = {"feed": FEED_URL, "fetched_at": utc_now_iso(), "items": items}
    put_text(CONTAINER, _raw_base() + "rss.json", json.dumps(raw_payload, ensure_ascii=False, indent=2), "application/json")

    # 4) Skriv CURATED + manifest
    curated_payload = {
        "league": LEAGUE,
        "feed": FEED_URL,
        "fetched_at": utc_now_iso(),
        "items": items,
    }
    put_text(CONTAINER, _curated_base() + "items.json", json.dumps(curated_payload, ensure_ascii=False, indent=2), "application/json")

    manifest = {"source": FEED_NAME, "count": count, "generated_at": utc_now_iso()}
    put_text(CONTAINER, _curated_base() + "input_manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2), "application/json")

    print(f"🏁 [rss] done → wrote to {_curated_base()} (items={count})")

if __name__ == "__main__":
    main()
