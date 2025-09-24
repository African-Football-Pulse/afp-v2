import yaml, json

def load_voice_map(lang: str):
    with open("config/speaking_roles.yaml", "r", encoding="utf-8") as f:
        roles = yaml.safe_load(f)
    with open("config/voice_ids.json", "r", encoding="utf-8") as f:
        voice_ids = json.load(f)

    voice_map = {}
    # Om språket inte finns → fallback till engelska
    target_lang = lang if lang in roles["roles"]["duo_experts"] else "en"

    for role, key in roles["roles"]["duo_experts"].get(target_lang, {}).items():
        if key in voice_ids:
            voice_map[role] = voice_ids[key]

    return voice_map
