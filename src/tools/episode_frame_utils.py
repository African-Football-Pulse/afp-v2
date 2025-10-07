import yaml
import os

def load_frame():
    """
    Laddar intro/outro-konfiguration från config/episode_frame.yaml.
    """
    path = "config/episode_frame.yaml"
    if not os.path.exists(path):
        print("[episode_frame_utils] ⚠️ episode_frame.yaml saknas")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def insert_intro_outro(sections_meta, lang):
    """
    Infogar enbart outro-sektionen i slutet av manuset.
    Introsektioner (t.ex. S.GENERIC.INTRO.DAILY) hanteras redan via episode.jinja.
    """
    cfg = load_frame()
    outro_text = cfg.get("outro", {}).get(lang)
    enriched = list(sections_meta)

    if outro_text:
        enriched.append({
            "section_id": "EPISODE.OUTRO",
            "role": "news_anchor",
            "lang": lang,
            "text": outro_text,
            "duration_s": 6,
        })
        print("[episode_frame_utils] ✅ Added outro section.")
    else:
        print("[episode_frame_utils] ℹ️ No outro text found in config.")

    print(f"[episode_frame_utils] Total sections after outro insertion: {len(enriched)}")
    return enriched
