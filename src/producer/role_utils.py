# src/producer/role_utils.py
import yaml
from typing import Dict

def load_yaml(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def resolve_persona_for_role(pod_config: Dict, role: str) -> str:
    """
    Slår upp vilken persona som ska användas för en given roll i en podds role_map.
    Fallbacks:
      - Om role = news_anchor men inte finns i role_map → använd "AK".
      - Annars → använd första i personas_active.
    """
    role_map = pod_config.get("role_map", {})
    personas_active = pod_config.get("personas_active", [])

    # 1. Direkt mapping
    if role in role_map:
        return role_map[role]

    # 2. Fallback för news_anchor
    if role == "news_anchor":
        return "AK"

    # 3. Fallback till första aktiva persona
    if personas_active:
        return personas_active[0]

    # 4. Ingen träff
    raise ValueError(f"No persona found for role '{role}' in pod config")
