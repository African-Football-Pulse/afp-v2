# src/producer/role_utils.py
import yaml
from typing import Dict

def load_yaml(path: str) -> Dict:
    """
    Load a YAML file and return its content as a dict.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def resolve_persona_for_role(pod_config: Dict, role: str) -> str:
    """
    Resolve which persona should be used for a given role in a pod's configuration.

    Lookup order:
      1. Use the pod's role_map if role is explicitly defined.
      2. If role is 'news_anchor' and missing from role_map → fallback to 'AK'.
      3. Otherwise → fallback to the first persona in personas_active.
      4. If nothing found → raise error.

    This ensures we never block execution, even if mappings are incomplete.
    """
    role_map = pod_config.get("role_map", {})
    personas_active = pod_config.get("personas_active", [])

    # 1. Direct mapping
    if role in role_map:
        persona_id = role_map[role]
        print(f"[role_utils] Role '{role}' resolved via role_map → {persona_id}")
        return persona_id

    # 2. Fallback for news_anchor
    if role == "news_anchor":
        print(f"[role_utils] Role '{role}' not in role_map → fallback to AK")
        return "AK"

    # 3. Fallback to first active persona
    if personas_active:
        persona_id = personas_active[0]
        print(f"[role_utils] Role '{role}' not mapped → fallback to first active persona {persona_id}")
        return persona_id

    # 4. Nothing found
    raise ValueError(f"[role_utils] No persona found for role '{role}' in pod config")
