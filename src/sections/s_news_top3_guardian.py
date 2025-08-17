# src/sections/s_news_top3_guardian.py
import os, json, datetime
from src.storage.azure_blob import put_text, get_text
from src.storage.hash_util import hash_dict

# --- Parametrar via env (med rimliga defaults)
CONTAINER   = os.getenv("AZURE_CONTAINER", "afp")
LEAGUE      = os.getenv("LEAGUE", "premier_league")
SECTION_ID  = os.getenv("SECTION_ID", "S.NEWS.TOP3_GUARDIAN")
LANG        = os.getenv("LANG", "en")
FEED_NAME   = os.getenv("FEED_NAME", "guardian_football")
TOP_N       = int(os.getenv("TOP_N", "3"))

def today_utc() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

def curated_items_blob(date: str) -> str:
    # Matchar collecterns layout: curated/news/{source}/{league}/{date}/items.json
    return f"curated/news/{FEED_NAME}/{LEAGUE}/{date}/items.json"

def load_items(path: str):
    raw = json.loads(get_text(CONTAINER, path))
    # Ta TOP_N första för enkelhet (kan bytas mot bättre rankning senare)
    items = raw.get("items", [])[:TOP_N]
    return items, raw

def render_text(items):
    lines = ["Here are the top headlines today:"]
    for it in items:
        title = it.get("title", "").strip()
        src   = it.get("source", "")
        link  = it.get("link", "")
        lines.append(f"- {title} ({src}) — {link}")
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
