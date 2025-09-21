# src/producer/produce_candidates.py
import os, json, uuid
from datetime import datetime, timezone

from src.storage import azure_blob

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")


def today_str():
    """Returnera dagens datum i UTC (YYYY-MM-DD)."""
    return datetime.now(timezone.utc).date().isoformat()


def load_master_players():
    path = "config/master_players.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_curated_news():
    """Ladda alla curated news items från Azure."""
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


# ---- scoring helpers ---------------------------------------------------
SOURCE_AUTHORITY = {
    "bbc_football": 0.85,
    "guardian_football": 0.85,
    "sky_sports_premier_league": 0.85,
    "independent_football": 0.7,
    # klubbfeeds kan sättas till 0.9
}

def compute_score(c):
    return round(
        0.35 * c.get("direct_player_mention", 0)
        + 0.25 * c.get("event_importance", 0)
        + 0.15 * c.get("source", {}).get("authority", 0.5)
        + 0.15 * c.get("recency_score", 0)
        + 0.05 * c.get("novelty_24h", 0)
        + 0.05 * c.get("language_match", 1.0),
        3,
    )
# ------------------------------------------------------------------------


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
        # fallback: check if club appears in source/feed name
        for mp in master_players:
            if mp.get("club", "").lower() in src.lower():
                player = mp
                direct_mention = 0.4
                break

    if not player:
        return None

    # recency (i timmar, relativt UTC)
    published = item.get("published_iso")
    recency_score = 0
    if published:
        try:
            dt = datetime.fromisoformat(published)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            recency_score = max(0, 1 - hours / 48)
        except Exception:
            pass

    cand = {
        "id": str(uuid.uuid4()),
        "player": player,
        "event": {"type": "news"},
        "source": {
            "name": src,
            "url": item.get("link"),
            "authority": SOURCE_AUTHORITY.get(src, 0.5),
        },
        "direct_player_mention": direct_mention,
        "event_importance": 0.0,
        "recency_score": recency_score,
        "novelty_24h": 1,  # TODO: implementera riktig dedupe/novelty
        "language_match": 1.0,
    }
    cand["score"] = compute_score(cand)
    return cand


def main():
    day = today_str()
    print(f"[produce_candidates] START day={day} (UTC)")

    master_players = load_master_players()
    print(f"[produce_candidates] Loaded {len(master_players)} master players")

    news_items = load_curated_news()
    print(f"[produce_candidates] Loaded {len(news_items)} news items from Azure")

    # logga antal items per källa
    per_source = {}
    for item in news_items:
        src = item.get("source", "unknown")
        per_source[src] = per_source.get(src, 0) + 1
    for src, count in per_source.items():
        print(f"  - {src}: {count} items")

    # skapa kandidater
    candidates = []
    for item in news_items:
        cand = candidate_from_news(item, master_players)
        if cand:
            candidates.append(cand)

    print(f"[produce_candidates] Built {len(candidates)} candidates")
    if candidates:
        scores = [c["score"] for c in candidates]
        print(
            f"[produce_candidates] Score stats: "
            f"min={min(scores):.2f} max={max(scores):.2f} avg={sum(scores)/len(scores):.2f}"
        )

    # spara JSONL
    out_path = f"producer/candidates/{day}/candidates.jsonl"
    text = "\n".join(json.dumps(c, ensure_ascii=False) for c in candidates)
    azure_blob.put_text(CONTAINER, out_path, text, content_type="application/jsonl")

    print(f"[produce_candidates] Wrote {out_path}")
    print("[produce_candidates] DONE")


if __name__ == "__main__":
    main()

