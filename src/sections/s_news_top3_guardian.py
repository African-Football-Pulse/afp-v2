# src/sections/s_news_top3_guardian.py
import os, json, datetime
from src.storage.azure_blob import put_text, get_text
from src.storage.hash_util import hash_dict

# --- Parameters via env (with sensible defaults)
CONTAINER   = os.getenv("AZURE_CONTAINER", "afp")
LEAGUE      = os.getenv("LEAGUE", "premier_league")
SECTION_ID  = os.getenv("SECTION_ID", "S.NEWS.TOP3_GUARDIAN")
LANG        = os.getenv("LANG", "en")
FEED_NAME   = os.getenv("FEED_NAME", "guardian_football")
TOP_N       = int(os.getenv("TOP_N", "3"))

def today_utc() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

def curated_items_blob(date: str) -> str:
    # Matches collector layout: curated/news/{source}/{league}/{date}/items.json
    return f"curated/news/{FEED_NAME}/{LEAGUE}/{date}/items.json"

def load_items(path: str):
    """
    Reads JSON from blob and returns (items_list, raw_json).
    Supports both:
      - {"items": [...]}  (dict with items)
      - [...]             (list directly)
    """
    raw_text = get_text(CONTAINER, path)
    raw = json.loads(raw_text)

    # Support both formats
    if isinstance(raw, dict):
        items = raw.get("items", [])
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    # Normalize defensively
    normalized = []
    for it in items:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or it.get("headline") or it.get("name") or "").strip()
        link = it.get("link") or it.get("url") or ""
        src  = it.get("source") or it.get("site") or "The Guardian"
        published = it.get("published") or it.get("pubDate") or it.get("date") or ""
        normalized.append({
            "title": title,
            "link": link,
            "source": src,
            "published": published,
        })

    # Apply TOP_N
    top = normalized[:TOP_N] if TOP_N > 0 else normalized
    return top, raw

def render_text(items):
    lines = ["Here are the top headlines today:"]
    for it in items:
        title = it.get("title", "").strip()
        src   = it.get("source", "")
        link  = it.get("link", "")
        lines.append(f"- {title} ({src}) — {link}")
    if not items:
        lines.append("- (no items available)")
    lines.append("More later.")
    return "\n".join(lines)

def main():
    date = today_utc()
    path = curated_items_blob(date)
    print(f"[section] league={LEAGUE} feed={FEED_NAME} lang={LANG} top_n={TOP_N}")
    items, raw = load_items(path)

    text = render_text(items)
    base = f"sections/{date}/{LEAGUE}/_/{SECTION_ID}/{LANG}/"

    manifest = {
        "section_id": SECTION_ID,
        "version": 1,
        "lang": LANG,
        "date": date,
        "league": LEAGUE,
        "inputs": {"curated_ref": path},
        "inputs_hash": hash_dict(raw),
        "target_duration_s": 45,
        "count": len(items),
    }

    put_text(CONTAINER, base + "section.txt", text, "text/plain; charset=utf-8")
    put_text(CONTAINER, base + "section_manifest.json",
             json.dumps(manifest, ensure_ascii=False, indent=2),
             "application/json")
    print(f"[section] wrote files under {base}")

if __name__ == "__main__":
    main()
