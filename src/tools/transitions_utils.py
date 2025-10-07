import os
import yaml

def load_transitions():
    """Laddar övergångskonfigurationen från config/transitions.yaml"""
    path = "config/transitions.yaml"
    if not os.path.exists(path):
        print("[transitions_utils] ⚠️ transitions.yaml saknas, hoppar över övergångar")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def insert_transitions(sections_meta, lang):
    """
    Infogar övergångssektioner mellan sektioner baserat på konfigurationen.
    """
    cfg = load_transitions()
    section_trans = cfg.get("section_transitions", {})
    role_trans = cfg.get("role_transitions", {})

    enriched = []
    for i, sec in enumerate(sections_meta):
        enriched.append(sec)
        if i < len(sections_meta) - 1:
            next_sec = sections_meta[i + 1]

            # Bygg nycklar för lookup
            key = f"{sec['section_id']}→{next_sec['section_id']}"
            role_key = f"{sec.get('role', 'news_anchor')}→{next_sec.get('role', 'news_anchor')}"

            # Prioritera sektion-övergång, fallback till roll-övergång
            text = section_trans.get(key, {}).get(lang)
            if not text:
                text = role_trans.get(role_key, {}).get(lang)

            if text:
                enriched.append({
                    "section_id": f"TRANSITION.{sec['section_id']}.{next_sec['section_id']}",
                    "role": "news_anchor",
                    "lang": lang,
                    "text": text,
                    "duration_s": 4
                })

    print(f"[transitions_utils] Inserted {len(enriched) - len(sections_meta)} transitions")
    return enriched
