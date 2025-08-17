import os, json, datetime
from src.storage.azure_blob import put_text, list_prefix
from src.storage.hash_util import hash_dict

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")
LEAGUE = "premier_league"
SECTION_ID = "S.NEWS.TOP3_GUARDIAN"
LANG = "en"

def today():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

def latest_curated_path(date: str):
    base = f"curated/news/guardian_football/{LEAGUE}/{date}/"
    # enkel kontroll – kräver items.json
    return base + "items.json"

def load_items(path: str):
    from src.storage.azure_blob import get_text
    data = json.loads(get_text(CONTAINER, path))
    # Ta tre första (vi förfinar sortering senare)
    return data["items"][:3], data

def render_text(items):
    lines = [
        "Here are the top three headlines today:",
    ]
    for it in items:
        lines.append(f"- {it['title']} ({it.get('source','')}) — {it['link']}")
    lines.append("More later.")
    return "\n".join(lines)

def main():
    date = today()
    curated_path = latest_curated_path(date)
    items, raw = load_items(curated_path)
    text = render_text(items)

    base = f"sections/{date}/{LEAGUE}/_/{SECTION_ID}/{LANG}/"
    manifest = {
        "section_id": SECTION_ID,
        "version": 1,
        "lang": LANG,
        "date": date,
        "league": LEAGUE,
        "inputs": {"curated_ref": curated_path},
        "inputs_hash": hash_dict(raw),
        "target_duration_s": 45,
    }
    put_text(CONTAINER, base + "section.txt", text, "text/plain; charset=utf-8")
    put_text(CONTAINER, base + "section_manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2),
             "application/json")

if __name__ == "__main__":
    main()
