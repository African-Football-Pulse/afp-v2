# src/sections/s_top_african_players/logic/select.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import os

from .scoring import recency_weight, event_boost, source_weight
# NYTT: lexikonet används för att avgöra om namn är afrikan samt för klubb/land
from ..lexicon import find as lex_find  # kräver: src/sections/s_top_african_players/lexicon.py

# Länder-lista används för stats-fallet (om nationality finns)
AFRICAN_COUNTRIES = {
    "nigeria","ghana","senegal","egypt","morocco","algeria","tunisia","ivory coast","cote d'ivoire",
    "cameroon","south africa","mali","burkina faso","guinea","congo","dr congo","gambia",
    "tanzania","uganda","kenya","zambia","zimbabwe","angola","benin","ethiopia","sudan","somalia"
}

TOP6_DEFAULT = {
    "Arsenal","Manchester City","Liverpool","Manchester United","Chelsea","Tottenham",
    "Spurs","Man City","Man United"
}

# ---- heuristik för att bara släppa igenom personnamn ----
_BAD_TOKENS = {
    "Palace","Forest","Villa","United","City","Rangers","Celtic","Hotspur","Park","Bridge","Arena","St","James",
    "Football","Weekly","Podcast","Guardian","Independent","BBC","Sky","Telegraph","Times","Athletic",
    "League","Premier","Champions","Europa","Conference","Cup","FA","UEFA","FIFA",
    "Arsenal","Chelsea","Liverpool","Newcastle","Everton","Tottenham","Spurs","Manchester",
    "West","Ham"  # blockera "West Ham" via tokens
}
_BAD_ENDINGS = {"FC","CF","SC","AC"}
_BAD_PHRASES = {"St James", "Old Firm", "West Ham", "Football Weekly"}

def _looks_like_person(name: str) -> bool:
    """Minimal falsk-positiv-gate (klubbar/arenor etc. filtreras bort)."""
    if not name:
        return False
    low = name.lower()
    for ph in _BAD_PHRASES:
        if ph.lower() in low:
            return False
    parts = name.split()
    if len(parts) < 2 or len(parts) > 3:
        return False
    if parts[-1] in _BAD_ENDINGS:
        return False
    if any(p in _BAD_TOKENS for p in parts):
        return False
    # enkel kapitaliseringskontroll (tillåt bindestreck)
    for p in parts:
        core = p.replace("-", "")
        if not core or not core[0].isupper() or not core[1:].islower():
            return False
    return True

# ---- klubb-gissning från rubrik/summary (fallback om lexikon saknar klubb) ----
CLUB_HINTS = [
    "Arsenal","Aston Villa","Bournemouth","Brentford","Brighton","Chelsea","Crystal Palace","Everton",
    "Fulham","Ipswich","Leicester","Liverpool","Man City","Manchester City","Man United","Manchester United",
    "Newcastle","Nottingham Forest","Southampton","Tottenham","Spurs","West Ham","Wolves","Wolverhampton"
]

def _infer_club_from_text(title: str, summary: str) -> Optional[str]:
    s = f"{title or ''} {summary or ''}"
    for club in CLUB_HINTS:
        if club and club in s:
            return club
    return None

# ---- lexikon-hjälpare ------------------------------------------------------

def _africa_cfg(ctx: Dict[str, Any]) -> Dict[str, Any]:
    return ctx.get("config", {}).get("top_african_players", {}).get("africa", {}) or {}

def _lexicon_path(ctx: Dict[str, Any]) -> str:
    return _africa_cfg(ctx).get("lexicon_path", "config/players_africa.json")

def _lexicon_only(ctx: Dict[str, Any]) -> bool:
    # om True → bara namn som finns i lexikonet räknas
    return bool(_africa_cfg(ctx).get("whitelist_only", True))  # behåller gamla nyckelnamn för kompat.

def _lexicon_boost(ctx: Dict[str, Any]) -> float:
    # liten bonus till träffar som finns i lexikonet
    try:
        return float(_africa_cfg(ctx).get("boost", 0.0))
    except Exception:
        return 0.0

def _is_african_name(name: str, ctx: Dict[str, Any]) -> bool:
    """Afrika-avgörande via lexikon; fallback: släpp om whitelist_only=False."""
    rec = lex_find(name, path=_lexicon_path(ctx))
    if rec:
        return True
    return not _lexicon_only(ctx)  # om inte strikt lexikon-läge → tillåt

def _canon_enrich(name: str, club: Optional[str], ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalisera namn mot lexikon och fyll klubb/land om känt.
    Returnerar dict: {"name": ..., "club": ..., "country": ...}
    """
    rec = lex_find(name, path=_lexicon_path(ctx))
    out = {"name": name, "club": club, "country": None}
    if rec:
        out["name"] = rec.get("name", name) or name
        if not club:
            out["club"] = rec.get("club") or club
        out["country"] = rec.get("country")
    return out

# ---------------------------------------------------------------------------

def pick_top_from_stats(stats: List[Dict[str, Any]], top_n: int, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Stats-baserad ranking. Afrikakoll görs via nationality eller lexikonträff på namn.
    """
    def is_african(rec: Dict[str, Any]) -> bool:
        nat = (rec.get("nationality") or "").lower().strip()
        if nat and nat in AFRICAN_COUNTRIES:
            return True
        name = (rec.get("name") or "").strip()
        return _is_african_name(name, ctx)

    def score(p):
        return (
            (p.get("goals",0)*4) +
            (p.get("assists",0)*3) +
            (p.get("xg",0.0)*1.5) +
            (p.get("xa",0.0)*1.0) +
            min(p.get("minutes",0),90)/90.0
        )

    min_min = int(ctx.get("config",{}).get("top_african_players",{}).get("min_minutes",30))
    cand = [p for p in (stats or []) if (p.get("minutes") or 0) >= min_min and is_african(p)]
    ranked = sorted(cand, key=score, reverse=True)[:top_n]

    # Namn/klubb-normalisering via lexikon
    out: List[Dict[str, Any]] = []
    for p in ranked:
        nm = (p.get("name") or "").strip()
        club = p.get("club")
        enr = _canon_enrich(nm, club, ctx)
        out.append({**p, "name": enr["name"], "club": enr.get("club")})
    return out

def pick_top_from_news(items: List[Dict[str, Any]], top_n: int, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Lexikon-drivet nyhetsurval:
      - Person-heuristik filtrerar brus (klubbnamn/arenor etc.)
      - Afrikafilter: namn måste finnas i lexikon *om* whitelist_only=True
      - Namn/klubb berikas via lexikon; klubb gissas från rubrik om saknas
      - Score = frekvens + source-weight + recency + event-boost (+ ev. lexikon-boost)
      - Tie-breakers: fler källor > top-6 klubb > alfabetiskt
    """
    from collections import defaultdict
    if not items:
        return []

    cfg = ctx.get("config",{}).get("top_african_players",{}) or {}
    top6 = set(cfg.get("top6_clubs", list(TOP6_DEFAULT)))
    lex_boost = _lexicon_boost(ctx)

    agg: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"score":0.0,"freq":0,"club":None,"item_ids":[],"sources":set(),"sample_title":None}
    )

    for it in items:
        # items kan ha players antingen toppnivå eller under entities.players
        plist = it.get("players") or it.get("entities", {}).get("players") or []
        if not plist:
            continue

        sid = it.get("id")
        src = it.get("source") or it.get("publisher") or it.get("domain")
        title = it.get("title") or ""
        summary = it.get("summary") or it.get("description") or ""
        published_at = it.get("published_at") or it.get("published_iso") or it.get("published")

        r_w = recency_weight(published_at, ctx)
        s_w = source_weight(src, ctx)
        e_b = event_boost(f"{title} {summary}", ctx)
        base_mention = 1.0 + 0.3*s_w + 0.2*r_w + 0.5*e_b

        for raw_name in plist:
            name = (raw_name or "").strip()
            # 1) person-heuristik (om lexikon *inte* redan vet om namnet)
            #    → om namnet finns i lexikon släpper vi igenom även om heuristiken skulle säga nej.
            in_lex = bool(lex_find(name, path=_lexicon_path(ctx)))
            if not in_lex and not _looks_like_person(name):
                continue

            # 2) Afrika-filter via lexikon (respekterar whitelist_only)
            if not _is_african_name(name, ctx):
                continue

            # 3) Namn/klubb via lexikon (canonical name + ev. klubb)
            club_initial = it.get("club")
            enr = _canon_enrich(name, club_initial, ctx)
            canon_name = enr["name"]
            club = enr.get("club") or _infer_club_from_text(title, summary)

            # 4) Score (lexikonträff ger liten boost)
            mention_score = base_mention + (lex_boost if in_lex else 0.0)

            a = agg[canon_name]
            a["score"] += mention_score
            a["freq"] += 1
            a["club"] = a["club"] or club
            if sid: a["item_ids"].append(sid)
            if src: a["sources"].add(str(src))
            a["sample_title"] = title or a["sample_title"]

    if not agg:
        return []

    ranked = sorted(
        agg.items(),
        key=lambda kv: (
            -round(kv[1]["score"], 6),
            -len(kv[1]["sources"]),
            -(1 if (kv[1].get("club") in top6) else 0),
            kv[0].lower()
        )
    )[:top_n]

    return [
        {
            "name": n,
            "club": m.get("club"),
            "score": round(m["score"], 3),
            "freq": m["freq"],
            "item_ids": m["item_ids"],
            "num_sources": len(m["sources"]),
            "sample_title": m["sample_title"]
        }
        for n, m in ranked
    ]
