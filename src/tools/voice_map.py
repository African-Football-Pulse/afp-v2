import yaml, json

def load_voice_map(lang: str):
    with open("config/speaking_roles.yaml", "r", encoding="utf-8") as f:
        roles = yaml.safe_load(f)
    with open("config/voice_ids.json", "r", encoding="utf-8") as f:
        voice_ids = json.load(f)

    voice_map = {}
    if lang in roles["roles"]["duo_experts"]:
        for role, key in roles["roles"]["duo_experts"][lang].items():
            voice_map[role] = voice_ids[key]
    return voice_map
