"""
transitions_utils.py
-------------------------------------
Hanterar övergångar ("transitions") mellan sektioner i manuset.

- Läser config/transitions.yaml
- Infogar automatiskt korta övergångsblock mellan sektioner
- Faller tillbaka till rollbaserade övergångar om ingen exakt sektionsträff finns
-------------------------------------
"""

import os
import yaml
import logging

# ---------- Logger ----------
logger = logging.getLogger("transitions_utils")
handler = logging.StreamHandler()
formatter = logging.Formatter("[transitions_utils] %(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler]
logger.setLevel(logging.INFO)
logger.propagate = False


def load_transitions():
    """Laddar övergångskonfigurationen från config/transitions.yaml"""
    path = "config/transitions.yaml"
    if not os.path.exists(path):
        logger.warning("⚠️ transitions.yaml saknas – hoppar över övergångar")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            logger.info("Loaded transitions config (%s)", path)
            return data
    except Exception as e:
        logger.error("Kunde inte läsa transitions.yaml: %s", e)
        return {}


def insert_transitions(sections_meta, lang):
    """
    Infogar övergångssektioner mellan sektioner baserat på config/transitions.yaml.

    🔹 För varje par av sektioner kontrolleras:
        1. Om det finns en exakt sektion→sektion-övergång
        2. Om inte, testas roll→roll-fallback
    """
    cfg = load_transitions()
    section_trans = cfg.get("section_transitions", {})
    role_trans = cfg.get("role_transitions", {})

    enriched = []
    added_count = 0

    for i, sec in enumerate(sections_meta):
        enriched.append(sec)
        if i >= len(sections_meta) - 1:
            continue

        next_sec = sections_meta[i + 1]

        # Bygg lookup-nycklar
        key = f"{sec['section_id']}→{next_sec['section_id']}"
        role_key = f"{sec.get('role', 'news_anchor')}→{next_sec.get('role', 'news_anchor')}"

        # Försök hämta text för övergång
        text = section_trans.get(key, {}).get(lang) or role_trans.get(role_key, {}).get(lang)
        if text:
            trans_id = f"TRANSITION.{sec['section_id']}.{next_sec['section_id']}"
            enriched.append({
                "section_id": trans_id,
                "role": "news_anchor",
                "lang": lang,
                "text": text,
                "duration_s": 4,
            })
            added_count += 1
            logger.debug("Added transition: %s (%s...)", trans_id, text[:50])

    logger.info("Inserted %d transitions", added_count)
    return enriched
