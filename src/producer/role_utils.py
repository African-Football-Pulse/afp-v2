# src/producer/role_utils.py
import yaml
from typing import Dict


def load_yaml(path: str) -> Dict:
    """
    Load a YAML file and return its content as a dict.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_pod_config(pod_name: str) -> Dict:
    """
    Load pods.yaml and return the config for a specific pod.
    """
    pods_cfg = load_yaml("config/pods.yaml")["pods"]
    if pod_name not in pods_cfg:
        raise ValueError(f"[role_utils] Pod '{pod_name}' not found in config/pods.yaml")
    return pods_cfg[pod_name]


def resolve_persona_for_role(pod_config: Dict, role: str) -> str:
    """
    Resolve which persona should be used for a given role in a pod's configuration.

    Lookup order:
      1. Use the pod's role_map if role is explicitly defined.
      2. If role is 'news_anchor' and missing from role_map → fallback to 'AK'.
      3. Otherwise → fallback to the first persona in personas_active.
      4. If nothing found → raise error.
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


def resolve_block_for_persona(persona_id: str) -> Dict:
    """
    Load speaking_roles.yaml and return the persona block for the given persona_id.
    """
    roles_cfg = load_yaml("config/speaking_roles.yaml")["roles"]

    # Linear search over all role categories
    for role_name, lang_map in roles_cfg.items():
        if isinstance(lang_map, dict):
            for lang, persona in lang_map.items():
                if isinstance(persona, dict):
                    if persona_id in persona.values():
                        return persona
                elif persona_id == persona:
                    return {"id": persona_id, "role": role_name, "lang": lang}

    raise ValueError(f"[role_utils] Persona '{persona_id}' not found in speaking_roles.yaml")
