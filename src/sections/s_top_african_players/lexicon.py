# src/sections/s_top_african_players/lexicon.py
from __future__ import annotations
from typing import Dict, Any, Optional
import json, os

_LEX_PATH = "config/players_africa.json"
_INDEX = None  # type: Optional[Dict[str, Dict[str, Any]]]

def _build_index(data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for p in data.get("players", []):
        names = [p.get("name","")]
        for a in p.get("aliases", []) or []:
            names.append(a)
        for n in names:
            n = (n or "").strip()
            if n:
                idx[n.lower()] = p
    return idx

def load_lexicon(path: str = None) -> Dict[str, Dict[str, Any]]:
    """Returnerar en name->record index (case-insensitiv). Cachas i minnet."""
    global _INDEX, _LEX_PATH
    if path:
        _LEX_PATH = path
    if _INDEX is None:
        try:
            with open(_LEX_PATH, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception:
            data = {"players": []}
        _INDEX = _build_index(data)
    return _INDEX

def find(name: str, path: str = None) -> Optional[Dict[str, Any]]:
    if not name: return None
    idx = load_lexicon(path)
    return idx.get(name.lower())
