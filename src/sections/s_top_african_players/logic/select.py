mkdir -p src/sections/s_top_african_players/logic

cat > src/sections/s_top_african_players/logic/select.py <<'PY'
from typing import Any, Dict, List
import os

from .scoring import recency_weight, event_boost, source_weight

AFRICAN = {
    "nigeria","ghana","senegal","egypt","morocco","algeria","tunisia","ivory coast","cote d'ivoire",
    "cameroon","south africa","mali","burkina faso","guinea","congo","dr congo","gambia",
    "tanzania","uganda","kenya","zambia","zimbabwe","angola","benin","ethiopia","sudan","somalia"
}
TOP6_DEFAULT = {"Arsenal","Manchester City","Liverpool","Manchester United","Chelsea","Tottenham",
                "Spurs","Man City","Man United"}

# ---- heuristik för att bara släppa igenom personnamn ----
_BAD_TOKENS = {
    "Palace","Forest","Villa","United","City","Rangers","Celtic","Hotspur","Park","Bridge","Arena","St","James",
    "Football","Weekly","Podcast","Guardian","Independent","BBC","Sky","Telegraph","Times","Athletic",
    "League","Premier","Champions","Europa","Conference","Cup","FA","UEFA","FIFA",
    "Arsenal","Chelsea","Liverpool","Newcastle","Everton","Tottenham","Spurs","Manchester",
    "West","Ham"
}
_BAD_ENDINGS = {"FC","CF","SC","AC"}
_BAD_PHRASES = {"St James", "Old Firm", "West Ham", "Football Weekly"}

def _looks_like_person(name: str) -> bool:
    if not name:
        return False
    for ph in _BAD_PHRASES:
        if ph.lower() in name.lower():
            return False
    parts = name.split()
    if len(parts) < 2 or len(parts) > 3:
        return False
    if parts[-1] in _BAD_ENDINGS:
        return False
    if any(p in _BAD_TOKENS for p in parts):
        return False
    for p in parts:
        core = p.replace("-", "")
        if not core or not core[0].isupper() or not core[1:].islower():
            return False
    return True

# ---- Afrika-whitelist ------------------------------------------------------
_DEFAULT_AFRICA_WHITELIST = {
    "Mohamed Salah","Sadio Mané","Riyad Mahrez","Achraf Hakimi","Kalidou Koulibaly","Thomas Partey",
    "Bukayo Saka","Victor Osimhen","Mohammed Kudus","Nicolas Jackson","Yoane Wissa","Taiwo Awoniyi",
    "Andre Onana","Edouard Mendy","Pierre-Emerick Aubameyang","Ismaïla Sarr","Sébastien Haller",
    "Sofyan Amrabat","Yves Bissouma","Alex Iwobi","Patson Daka","Eric Bailly","Naby Keïta","Noussair Mazraoui",
}

def _load_africa_whitelist(path: str = "config/player_lexicon_africa.txt") -> set:
    names = set(_DEFAULT_AFRICA_WHITELIST)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s and not s.startswith("#"):
                        names.add(s)
    except Exception:
        pass
    return names

# ---------------------------------------------------------------------------

def pick_top_from_stats(stats: List[Dict[str, Any]], top_n: int, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    def is_african(nat): return (not nat) or nat in AFRICAN
    def score(p):
        return (p.get("goals",0)*4)+(p.get("assists",0)*3)+(p.get("xg",0.0)*1.5)+(p.get("xa",0.0)*1.0)+min(p.get("minutes",0),90)/90.0
    min_min = int(ctx.get("config",{}).get("top_african_players",{}).get("min_minutes",30))
    cand = [p for p in stats if (p.get("minutes") or 0) >= min_min and is_african((p.get("nationality") or "").lower())]
    ranked = sorted(cand, key=score, reverse=True)[:top_n]
    return ranked

def pick_top_from_news(items: List[Dict[str, Any]], top_n: int, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Tvingar whitelist-läge: bara namn som finns i Afrika-whitelist räknas.
    """
    from collections import defaultdict
    if not items:
        return []

    top6 = set(ctx.get("config",{}).get("top_african_players",{}).get("top6_clubs", list(TOP6_DEFAULT)))
    whitelist = _load_africa_whitelist()  # alltid på
    whitelist_boost = float(ctx.get("config",{}).get("top_african_players",{}).get("africa",{}).get("boost", 0.0))

    agg = defaultdict(lambda: {"score":0.0,"freq":0,"club":None,"item_ids":[],"sources":set(),"sample_title":None})

    for it in items:
        plist = it.get("players") or it.get("entities", {}).get("players") or []
        if not plist:
            continue
        sid = it.get("id")
        src = it.get("source") or it.get("publisher") or it.get("domain")
        title = it.get("title") or ""
        summary = it.get("summary") or ""
        published_at = it.get("published_at")

        r_w = recency_weight(published_at, ctx)
        s_w = source_weight(src, ctx)
        e_b = event_boost(f"{title} {summary}", ctx)
        base_mention = 1.0 + 0.3*s_w + 0.2*r_w + 0.5*e_b

        for raw_name in plist:
            name = raw_name.strip()
            if not _looks_like_person(name):
                continue
            if name not in whitelist:
                continue
            mention_score = base_mention + (whitelist_boost if name in whitelist else 0.0)
            a = agg[name]
            a["score"] += mention_score
            a["freq"] += 1
            a["club"] = a["club"] or it.get("club")
            if sid: a["item_ids"].append(sid)
            if src: a["sources"].add(str(src))
            a["sample_title"] = title or a["sample_title"]

    if not agg:
        return []

    ranked = sorted(
        agg.items(),
        key=lambda kv: (-round(kv[1]["score"],6), -len(kv[1]["sources"]), -(1 if (kv[1].get("club") in top6) else 0), kv[0].lower())
    )[:top_n]
    return [
      {"name": n, "club": m.get("club"), "score": round(m["score"],3), "freq": m["freq"], "item_ids": m["item_ids"], "num_sources": len(m["sources"]), "sample_title": m["sample_title"]}
      for n,m in ranked
    ]
PY
