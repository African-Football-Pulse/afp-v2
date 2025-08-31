# sections/s_top_african_players/renderers/stats.py
from typing import Any, Dict, List, Tuple

def render_stats(players: List[Dict[str, Any]], lang: str="en", target_sec: int=50) -> Tuple[str, List[str]]:
    if not players:
        return ("No reliable stats this week — switching to news highlights." if lang.startswith("en")
                else "Inga pålitliga statistikdata denna vecka — vi växlar till nyhetsspaningar."), []
    lines = ["Top African performers this week:" if lang.startswith("en") else "Veckans bästa afrikanska prestationer:"]
    sources: List[str] = []
    for p in players:
        club = f" ({p.get('club')})" if p.get("club") else ""
        g  = f"{p.get('goals',0)}G"
        a  = f"{p.get('assists',0)}A"
        xg = p.get("xg"); xa = p.get("xa")
        xg_s = f"xG {xg:.2f}" if isinstance(xg, float) else "xG —"
        xa_s = f"xA {xa:.2f}" if isinstance(xa, float) else "xA —"
        m  = p.get("minutes"); m_s = f"{m}’" if m is not None else "—"
        lines.append(f"- {p.get('name','Unknown')}{club}: {g}, {a}, {xg_s}, {xa_s}, {m_s}")
        if p.get("source_ref"): sources.append(p["source_ref"])
    lines.append("More next time." if lang.startswith("en") else "Mer nästa gång.")
    return "\n".join(lines), sources
