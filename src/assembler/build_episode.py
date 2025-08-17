import json, os
from datetime import datetime
from src.storage.azure_blob import list_prefix, put_text

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def today():
    return datetime.utcnow().strftime("%Y-%m-%d")

def find_sections(date: str, league: str, lang: str):
    prefix = f"sections/{date}/{league}/_/"
    names = list_prefix(CONTAINER, prefix)
    manifests = [n for n in names if n.endswith("/section_manifest.json") and f"/{lang}/" in n]
    return manifests

def main():
    date = today()
    league = "premier_league"
    lang = "en"
    manifests = find_sections(date, league, lang)
    if not manifests:
        report = {"status": "no-episode", "reason": "no sections found", "date": date}
        put_text(CONTAINER, f"episodes/{date}/{league}/daily/{lang}/report.json", json.dumps(report), "application/json")
        return

    sections_meta = [{"section_id": m.split("/")[-3], "lang": lang, "duration_s": 60} for m in manifests]
    script = {
        "pod_id": "afp-premier-league-daily-en",
        "date": date,
        "type": "micro",
        "lang": lang,
        "jingles": {"intro": "jingles/J2.mp3", "outro": "jingles/J2.mp3"},
        "sections": sections_meta,
        "duration_s": sum(s["duration_s"] for s in sections_meta)
    }
    base = f"episodes/{date}/{league}/daily/{lang}/"
    put_text(CONTAINER, base + "episode_manifest.json", json.dumps(script, indent=2), "application/json")
    put_text(CONTAINER, base + "episode_script.txt", "Micro episode (stub) – stitching in Sprint 3", "text/plain; charset=utf-8")

if __name__ == "__main__":
    main()
