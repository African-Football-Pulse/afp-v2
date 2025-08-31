# sections/s_top_african_players.py
"""
S1: S.STATS.TOP_AFRICAN_PLAYERS
Bygger en kort sektion (ca 45–60 s) som lyfter fram 3 afrikanska spelare i fokus.
- Primär källa: stats (om tillgängligt).
- Fallback: nyhetsflöden (normaliserade items) senaste dygnet/omgången.

Returnerar ett dict enligt AFP-sektions-API:
{
  "slug": "top_african_players",
  "title": "...",
  "text": "...",
  "length_s": 52,
  "sources": [...],
  "meta": {"persona": "JJ"}
}
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json
import math
import os

# ----------------------------
# Publikt API
# ----------------------------

def build(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    ctx förväntas innehålla:
      - league: str
      - lang: str (t.ex. "en")
      - collected_prefix: str (blob-prefix där collect-data ligger)  [valfritt men vanligt]
      - config: dict (kan innehålla thresholds, top_n, mm)            [valfritt]
      - target_length_s: int                                         [valfritt]
      - stats_prefix: str (om stats ligger separat)                   [valfritt]
      - now_iso: str (för tidsstämplar i logik, om ni behöver)        [valfritt]
    """
    league = ctx.get("league", "premier-league")
    lang = ctx.get("lang", "en")
    target_len = int(ctx.get("target_length_s", 50))
    top_n = int(_get(ctx, "config.top_african_players.top_n", 3))
    min_minutes = int(_get(ctx, "config.top_african_players.min_minutes", 30))

    # 1) Försök stats
    stats = try_load_stats(ctx)
    if stats:
        players = pick_top_africans_from_stats(stats, top_n=top_n, min_minutes=min_minutes)
        if players:
            text, sources = render_players_from_stats(players, lang=lang, target_sec=target_len)
            return _make_payload(text, target_len, sources)

    # 2) Fallback: nyheter
    items = load_normalized_items(ctx)
    players = infer_players_from_news(items, top_n=top_n)
    text, sources = render_players_from_news(players, lang=lang, target_sec=target_len)
    return _make_payload(text, target_len, sources)


# ----------------------------
# Hjälpstrukturer & utilities
# ----------------------------

@dataclass
class PlayerStat:
    name: str
    club: Optional[str] = None
    minutes: Optional[int] = None
    xg: Optional[float] = None
    xa: Optional[float] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    shots: Optional[int] = None
    nationality: Optional[str] = None
    source_ref: Optional[str] = None   # fil/objekt-id/källa

AFRICAN_COUNTRIES = {
    # Kort lista (kan byggas ut). Sektionen fungerar även utan nationality-filter.
    "nigeria","ghana","senegal","egypt","morocco","algeria","tunisia","ivory coast","cote d'ivoire",
    "cameroon","south africa","mali","burkina faso","guinea","congo","dr congo","gambia",
    "tanzania","uganda","kenya","zambia","zimbabwe","angola","benin","ethiopia","sudan","somalia"
}

def _get(d: Dict[str, Any], dotted: str, default: Any=None) -> Any:
    cur = d
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def estimate_length(text: str, fallback: int = 50) -> int:
    # Grov uppskattning: ~150 ord/minut (2.5 ord/sek). Lägg på liten buffert.
    words = max(1, len(text.split()))
    seconds = int(round(words / 2.5))
    return seconds if abs(seconds - fallback) > 5 else fallback

def _make_payload(text: str, target_len: int, sources: List[str]) -> Dict[str, Any]:
    return {
        "slug": "top_african_players",
        "title": "Top African Players this week",
        "text": text.strip(),
        "length_s": estimate_length(text, target_len),
        "sources": sources,
        "meta": {"persona": "JJ"}
    }

# ----------------------------
# Ladda stats
# ----------------------------

def try_load_stats(ctx: Dict[str, Any]) -> List[PlayerStat]:
    """
    Förväntar sig att stats kan ligga i t.ex.:
      - ctx["stats"] (redan injicerade data), eller
      - en JSON-fil under stats-prefix (t.ex. '.../stats/players.json'),
      - eller demo om ctx['config']['demo']['allow_demo'] == True.
    Returnerar lista av PlayerStat. Tom lista/None om ej tillgängligt.
    """
    # 1) Direkt injicerade
    if "stats" in ctx and isinstance(ctx["stats"], list):
        return _coerce_stats(ctx["stats"])

    # 2) Blob/fil—placeholder: ni kan ersätta med er verkliga loader.
    #    För att sektionen ska vara självständig läser vi INTE blob här.
    #    Lägg hellre stats i ctx["stats"] i produce-steget.
    # 3) Demo
    if _get(ctx, "config.demo.allow_demo", True):
        demo = {
            "league": ctx.get("league", "premier-league"),
            "players": [
                {"name": "Mohamed Salah", "club": "Liverpool", "xg": 0.6, "xa": 0.3, "minutes": 90, "goals": 1, "assists": 1, "nationality": "Egypt"},
                {"name": "Thomas Partey", "club": "Arsenal", "xg": 0.1, "xa": 0.2, "minutes": 78, "goals": 0, "assists": 0, "nationality": "Ghana"},
                {"name": "Andre Onana", "club": "Man United", "xg": 0.0, "xa": 0.0, "minutes": 90, "goals": 0, "assists": 0, "nationality": "Cameroon"}
            ]
        }
        return _coerce_stats(demo["players"])

    return []

def _coerce_stats(rows: Iterable[Dict[str, Any]]) -> List[PlayerStat]:
    out: List[PlayerStat] = []
    for r in rows:
        out.append(PlayerStat(
            name=r.get("name") or r.get("player") or "Unknown",
            club=r.get("club"),
            minutes=_safe_int(r.get("minutes")),
            xg=_safe_float(r.get("xg")),
            xa=_safe_float(r.get("xa")),
            goals=_safe_int(r.get("goals")),
            assists=_safe_int(r.get("assists")),
            shots=_safe_int(r.get("shots")),
            nationality=(r.get("nationality") or r.get("nation") or "").lower() or None,
            source_ref=r.get("source") or r.get("ref") or None
        ))
    return out

def _safe_int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except Exception:
        return None

# ----------------------------
# Rankning från stats
# ----------------------------

def pick_top_africans_from_stats(stats: List[PlayerStat], top_n: int = 3, min_minutes: int = 30) -> List[PlayerStat]:
    if not stats:
        return []

    def is_african(ps: PlayerStat) -> bool:
        if not ps.nationality:
            # Om nationality saknas → tillåt (vi vill hellre visa något).
            return True
        return ps.nationality in AFRICAN_COUNTRIES

    # Enkel score: mål & assist väger tyngst, sedan xG/xA och minuter
    def score(ps: PlayerStat) -> float:
        g = ps.goals or 0
        a = ps.assists or 0
        xg = ps.xg or 0.0
        xa = ps.xa or 0.0
        m = ps.minutes or 0
        # viktning
        return (g * 4.0) + (a * 3.0) + (xg * 1.5) + (xa * 1.0) + (min(m, 90) / 90.0)

    candidates = [p for p in stats if (p.minutes or 0) >= min_minutes and is_african(p)]
    ranked = sorted(candidates, key=score, reverse=True)
    return ranked[:top_n]

def render_players_from_stats(players: List[PlayerStat], lang: str = "en", target_sec: int = 50) -> Tuple[str, List[str]]:
    if not players:
        return _render_no_data(lang), []

    lines: List[str] = []
    if lang.startswith("en"):
        lines.append("Top African performers this week:")
    else:
        lines.append("Veckans bästa afrikanska prestationer:")

    sources: List[str] = []
    for p in players:
        sources.extend([p.source_ref] if p.source_ref else []]()
