# src/producer/produce_enrich_articles.py
import os
import json
import requests
from datetime import datetime, timezone
from readability import Document
from bs4 import BeautifulSoup

from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def log(msg: str):
    print(f"[produce_enrich_articles] {msg}", flush=True)


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def fetch_article_text(url: str) -> str:
    """Hämta och rensa artikeltext från URL"""
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "AFPBot/1.0"})
        resp.raise_for_status()
        doc = Document(resp.text)
        html = doc.summary()
        soup = BeautifulSoup(html, "html.parser")
        text = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p"))
        return text.strip()
    except Exception as e:
        log(f"WARN: failed to fetch {url} ({e})")
        return None


def main(top_n: int = 15):
    day = today_str()
    in_path = f"producer/candidates/{day}/scored.jsonl"
    out_path = f"producer/candidates/{day}/scored_enriched.jsonl"

    if not azure_blob.exists(CONTAINER, in_path):
        log(f"❌ Missing input: {in_path}")
        return

    raw_text = azure_blob.get_text(CONTAINER, in_path)
    scored = [json.loads(line) for line in raw_text.splitlines() if line.strip()]
    log(f"Loaded {len(scored)} scored items")

    # Sortera efter score och ta topp N
    scored = sorted(scored, key=lambda c: c.get("score", 0), reverse=True)
    top_items = scored[:top_n]

    enriched = []
    for c in top_items:
        url = c.get("source", {}).get("url")
        if url:
            article_text = fetch_article_text(url)
            if article_text:
                c["article_text"] = article_text
        enriched.append(c)

    text_out = "\n".join(json.dumps(c, ensure_ascii=False) for c in enriched)
    azure_blob.put_text(CONTAINER, out_path, text_out, content_type="application/json; charset=utf-8")

    log(f"Wrote {out_path} with {len(enriched)} items")
    log("DONE")


if __name__ == "__main__":
    # Tillåt ev. CLI-parametrar senare, men default kör topp 15
    main()
