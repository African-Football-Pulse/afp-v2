# src/producer/role_utils.py
import yaml
import os
from typing import Dict

def load_yaml(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def resolve_persona_for_role(pod_config: Dict, role: str) -> str:
    """
    Slår upp vilken persona som ska användas för en given roll i en podds role_map.
    """
    role_map = pod_config.get("role_map", {})
    persona_id = role_map.get(role)
    if not persona_id:
        raise ValueError(f"No persona mapped for role '{role}' in pod config.")
    return persona_id
