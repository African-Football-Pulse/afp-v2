# src/producer/produce_candidates.py
import os, json, uuid
from datetime import datetime, timezone

from src.storage import azure_blob

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def load_master_players():
    path = "config/master_players.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_curated_news():
    blob_paths = azure_blob.list_prefix(CONTAINER, "curated/news/")
    items = []
    for bp in blob_paths:
        if not bp.endswith("items.json"):
            continue
        try:
            data = azure_blob.get_json(CONTAINER, bp)
            for d in data:
                d["__blob_path"] = bp
            items.extend(data)
        except Exception as e:
            print(f"[WARN] Failed to load {bp}: {e}")
    return items


def candidate_from_news(item, master_players):
    title = (item.get("title") or "")
    summary = (item.get("summary") or "")
    text = f"{title} {summary}"
    src = item.get("source", "unknown")

    player = None
    direct_mention = 0
    for mp in master_players:
        if mp["name"].lower() in text.lower():
            player = mp
            direct_mention = 1
            break

    if not player:
        for mp in master_players:
            if mp.get("club", "").lower() in src.lower():
                player = mp
                direct_mention = 0.4
                break

    if not player:
        return None

    published = item.get("published_iso")
    published_utc = None
    if published:
        try:
            dt = datetime.fromisoformat(published)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            published_utc = dt.isoformat()
        except Exception:
            pass

    return {
        "id": str(uuid.uuid4()),
        "player": player,
        "event": {"type": "news"},
        "source": {
            "name": src,
            "url": item.get("link"),
        },
        "direct_player_mention": direct_mention,
        "event_importance": 0.0,
        "published_iso": published_utc,
        "recency_score": None,   # fylls i scoring-steget
        "novelty_24h": None,     # fylls i scoring-steget
        "language_match": None,  # fylls i scoring-steget
    }


def main():
    day = today_str()
    print(f"[produce_candidates] START day={day} (UTC)")

    master_players = load_master_players()
    print(f"[produce_candidates] Loaded {len(master_players)} master players")

    news_items = load_curated_news()
    print(f"[produce_candidates] Loaded {len(news_items)} news items from Azure")

    candidates = []
    for item in news_items:
        cand = candidate_from_news(item, master_players)
        if cand:
            candidates.append(cand)

    print(f"[produce_candidates] Built {len(candidates)} raw candidates")

    out_path = f"producer/candidates/{day}/candidates.jsonl"
    text = "\n".join(json.dumps(c, ensure_ascii=False) for c in candidates)
    azure_blob.put_text(CONTAINER, out_path, text, content_type="application/jsonl")

    print(f"[produce_candidates] Wrote {out_path}")
    print("[produce_candidates] DONE")


if __name__ == "__main__":
    main()
