#!/usr/bin/env python3
import json, yaml, pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
cfg_voice = ROOT / "config" / "voice_ids.json"
cfg_persona = ROOT / "config" / "persona.json"
cfg_roles = ROOT / "config" / "speaking_roles.yaml"

def main():
    voices = json.loads(cfg_voice.read_text(encoding="utf-8"))
    personas = json.loads(cfg_persona.read_text(encoding="utf-8"))
    roles = yaml.safe_load(cfg_roles.read_text(encoding="utf-8"))

    missing = []
    for key in ["ak", "jjk", "kem", "oes"]:
        if key not in voices:
            missing.append(key)

    if missing:
        print(f"⚠️ Saknar voice_id för: {', '.join(missing)}")

    # Lägg till default roller om de saknas
    roles.setdefault("roles", {})
    roles["roles"].setdefault("duo_experts", {
        "en_swahili": {"expert1": "ak", "expert2": "jjk"},
        "en_arabic": {"expert1": "oes", "expert2": "kem"},
    })

    # Skriv tillbaka
    cfg_roles.write_text(
        yaml.dump(roles, allow_unicode=True, sort_keys=False),
        encoding="utf-8"
    )
    print("✅ speaking_roles.yaml uppdaterad")

if __name__ == "__main__":
    sys.exit(main())
