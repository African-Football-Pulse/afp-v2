import yaml, os

def load_frame():
    path = "config/episode_frame.yaml"
    if not os.path.exists(path):
        print("[episode_frame_utils] ⚠️ episode_frame.yaml saknas")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def insert_intro_outro(sections_meta, lang):
    cfg = load_frame()
    intro_text = cfg.get("intro", {}).get(lang)
    outro_text = cfg.get("outro", {}).get(lang)
    enriched = []

    # Intro – endast om första sektionen inte redan börjar med “welcome”
    if intro_text:
        enriched.append({
            "section_id": "EPISODE.INTRO",
            "role": "news_anchor",
            "lang": lang,
            "text": intro_text,
            "duration_s": 6
        })

    enriched.extend(sections_meta)

    # Outro
    if outro_text:
        enriched.append({
            "section_id": "EPISODE.OUTRO",
            "role": "news_anchor",
            "lang": lang,
            "text": outro_text,
            "duration_s": 6
        })

    print(f"[episode_frame_utils] Added intro/outro (intro={bool(intro_text)}, outro={bool(outro_text)})")
    return enriched
