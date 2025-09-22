# src/sections/s_top_african_players/renderers/gpt.py
from typing import Dict, List, Tuple
import os, json

def _dedupe(seq):
    seen, out = set(), []
    for x in seq:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

def _first_sentence(t: str) -> str:
    if not t: return ""
    t = t.strip()
    for stop in [". ", " – ", " — ", " | "]:
        if stop in t:
            return t.split(stop, 1)[0].rstrip(".")
    return t[:160].rstrip(" .")

def _collect_links(players: List[Dict[str, any]], items: List[Dict[str, any]]) -> List[str]:
    id_to_link = {it.get("id"): (it.get("link") or it.get("url")) for it in (items or [])}
    links: List[str] = []
    for p in (players or []):
        for iid in p.get("item_ids", []):
            link = id_to_link.get(iid)
            if link:
                links.append(link)
    return _dedupe(links)

def _persona_from_ctx(ctx: Dict[str, any]) -> Dict[str, any]:
    """Hämta persona (AK default)."""
    persona = (ctx or {}).get("persona") or {}
    if persona:
        return persona
    # Fallback: minimal AK om inget laddats uppströms
    return {
        "key": "AK",
        "name": "Ama K (Amarachi Kwarteng)",
        "role": "Sports journalist & podcast host",
        "voice": "Clear, rhythmic British English with West African undertone",
        "tone": {"primary": "Factual, professional, driven", "secondary": "Witty, teasing"},
        "style": "Football terminology + journalism + pop culture",
        "catchphrases": [
            "JJ, I knew you’d say that. You're so predictable.",
            "This isn’t 1998 – we have data now."
        ],
        "traits": ["Cohesive","Outspoken","Analytical","Humorous","Relatable"]
    }

def _build_system_prompt(persona: Dict[str, any], use_catchphrases: bool) -> str:
    name = persona.get("name","Ama K")
    role = persona.get("role","Sports journalist")
    voice = persona.get("voice","British English")
    tone = persona.get("tone",{})
    primary = tone.get("primary","Factual, professional")
    secondary = tone.get("secondary","Witty")
    style = persona.get("style","Football journalism")
    catch = persona.get("catchphrases", []) if use_catchphrases else []

    lines = [
        f"You are {name}, {role}.",
        "Write in concise, crisp English suitable for a football podcast script.",
        f"Voice: {voice}. Tone: {primary}; second layer: {secondary}. Style: {style}.",
        "Only use facts I provide. No speculation, no invented details.",
        "Output format: a title on the first line, then exactly N bullets.",
        "Each bullet: '- Name (Club): one sharp sentence drawn from the headline.'",
        "No scores, no links, no emojis.",
        "Avoid over-stylizing. Keep it newsroom-clean and tight.",
    ]
    if catch:
        lines.append(f"Optional persona flavor (sparingly): {', '.join(catch)}")
    return "\n".join(lines)

def _build_messages(players: List[Dict[str, any]], persona: Dict[str, any], use_catchphrases: bool) -> List[Dict[str, any]]:
    facts = []
    for p in players:
        facts.append({
            "name": p.get("name"),
            "club": p.get("club"),
            "headline": _first_sentence(p.get("sample_title") or ""),
            "mentions": p.get("freq"),
            "sources": p.get("num_sources"),
        })
    system = _build_system_prompt(persona, use_catchphrases)
    title = "Top African names this week"
    user = {
        "role": "user",
        "content": [
            {"type": "input_text", "text":
                f"TITLE={title}\nN={len(players)}\nFACTS={json.dumps(facts, ensure_ascii=False)}\n"
                "Write ONLY the title and N bullets in English."
            }
        ],
    }
    return [{"role": "system", "content": system}, user]

def render_gpt(players: List[Dict[str, any]],
               lang: str = "en",
               target_sec: int = 50,
               ctx: Dict[str, any] | None = None) -> Tuple[str, List[str]]:
    """Returns (text, links). Falls back to rule-based if GPT fails."""
    items = (ctx or {}).get("items") or []
    links = _collect_links(players, items)
    if not players:
        return ("No clear African standouts in today’s headlines.", links)

    persona = _persona_from_ctx(ctx or {})
    use_catchphrases = bool((ctx or {}).get("config", {}).get("top_african_players", {}).get("nlg", {}).get("catchphrases", False))

    try:
        from openai import OpenAI  # pip install openai>=1.0
        model = (ctx or {}).get("config", {}).get("top_african_players", {}) \
                    .get("nlg", {}).get("model", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
        client = OpenAI()
        messages = _build_messages(players, persona, use_catchphrases)
        resp = client.responses.create(model=model, input=messages)
        text = resp.output_text.strip()

        # Ensure max N bullets
        N = len(players)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if lines:
            title = lines[0].strip()
            bullets = [ln for ln in lines[1:] if ln.lstrip().startswith(("-", "–", "—"))]
            if len(bullets) > N: bullets = bullets[:N]
            cleaned = [title] + [b if b.startswith("-") else f"- {b.lstrip('—– ')}" for b in bullets]
            text = "\n".join(cleaned).strip()
        return (text, links)
    except Exception:
        from .news import render_news
        return render_news(players, lang="en", target_sec=target_sec, ctx=ctx)
