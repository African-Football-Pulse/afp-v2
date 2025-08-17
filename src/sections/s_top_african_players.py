import json, os
from datetime import datetime
from src.storage.azure_blob import put_text
from src.storage.hash_util import hash_dict

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def today():
    return datetime.utcnow().strftime("%Y-%m-%d")

def load_curated_demo():
    return {
        "league": "premier_league",
        "players": [
            {"name": "Mohamed Salah", "club": "Liverpool", "xg": 0.6, "xa": 0.3, "minutes": 90},
            {"name": "Thomas Partey", "club": "Arsenal", "xg": 0.1, "xa": 0.2, "minutes": 78},
            {"name": "Andre Onana", "club": "Man United", "xg": 0.0, "xa": 0.0, "minutes": 90}
        ],
        "curated_ref": "curated/demo/premier_league/players.json"
    }

def render_text(data, lang="en"):
    intro = "Here are today’s top African performers in the Premier League:" if lang=="en" else "Dagens topprestationer:"
    lines = [intro]
    for p in data["players"]:
        lines.append(f"- {p['name']} ({p['club']}): xG {p['xg']}, xA {p['xa']}, minutes {p['minutes']}")
    lines.append("More tomorrow.")
    return "
".join(lines)

def main():
    date = today()
    league = "premier_league"
    lang = "en"
    section_id = "S.STATS.TOP_AFRICAN_PLAYERS"
    base = f"sections/{date}/{league}/_/{section_id}/{lang}/"

    data = load_curated_demo()
    text = render_text(data, lang)
    manifest = {
        "section_id": section_id,
        "version": 1,
        "lang": lang,
        "date": date,
        "league": league,
        "team": None,
        "personas": ["p1","p2"],
        "target_duration_s": 60,
        "inputs": {"curated_ref": data["curated_ref"]},
        "inputs_hash": hash_dict(data),
    }

    put_text(CONTAINER, base + "section.txt", text, "text/plain; charset=utf-8")
    put_text(CONTAINER, base + "section_manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2), "application/json")

if __name__ == "__main__":
    main()
