# src/sections/s_top_african_players/renderers/gpt.py
from typing import Dict, List, Tuple
import os

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

def _build_prompt(players: List[Dict[str, any]], lang: str) -> List[Dict[str, any]]:
    # Gör en kompakt, faktaburen sammanställning som input till modellen
    facts = []
    for p in players:
        facts.append({
            "name": p.get("name"),
            "club": p.get("club"),
            "headline": _first_sentence(p.get("sample_title") or ""),
            "mentions": p.get("freq"),
            "sources": p.get("num_sources"),
        })
    system_sv = (
        "Du är en sportjournalist. Skriv kort, precist och nyktert. "
        "Utgå enbart från faktan jag ger (inga egna antaganden). "
        "Output: en rubrik + exakt N punktlistor med 1 mening per spelare: "
        "‘Namn (Klubb): kort kärnpoäng från rubriken.’ "
        "Ingen poäng, inga källreferenser i texten, inga emojis."
    )
    system_en = (
        "You are a sports journalist. Write concise, factual one-sentence bullets per player. "
        "Use only the provided facts (no speculation). Output a title + exactly N bullets: "
        "‘Name (Club): tight takeaway from the headline.’ No scores, no links, no emojis."
    )
    title_sv = "Veckans afrikanska toppnamn"
    title_en = "Top African names this week"
    sys = system_sv if lang == "sv" else system_en
    title = title_sv if lang == "sv" else title_en
    # Vi ber modellen returnera bara brödtext (rubrik + N punkter)
    user = {
        "role": "user",
        "content": [
            {"type": "input_text", "text":
                f"LANG={lang}\nTITLE={title}\nN={len(players)}\nFACTS={facts}\n"
                "Skriv rubriken på första raden. Sedan N punkter med formatet:"
                "\n- Namn (Klubb): <en mening>."
            }
        ],
    }
    return [
        {"role": "system", "content": sys},
        user,
    ]

def render_gpt(players: List[Dict[str, any]],
               lang: str = "sv",
               target_sec: int = 50,
               ctx: Dict[str, any] | None = None) -> Tuple[str, List[str]]:
    """
    Returnerar (text, links). Faller tillbaka till regelbaserad renderer om GPT saknas/felar.
    Kräver: OPENAI_API_KEY i env och openai>=1.0.
    """
    items = (ctx or {}).get("items") or []
    links = _collect_links(players, items)

    # Tomt underlag -> tom text
    if not players:
        return (
            "Inga tydliga afrikanska toppnamn i dagens rubriker." if lang == "sv"
            else "No clear African standouts in today’s headlines."
        , links)

    # Försök GPT
    try:
        from openai import OpenAI  # pip install openai
        model = (ctx or {}).get("config", {}).get("top_african_players", {}) \
                    .get("nlg", {}).get("model", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
        client = OpenAI()
        messages = _build_prompt(players, lang)
        resp = client.responses.create(
            model=model,
            input=messages,
        )
        text = resp.output_text.strip()
        # Liten sanering: säkerställ max N punkter
        # (om modellen råkar skriva fler)
        N = len(players)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if lines:
            title = lines[0].strip()
            bullets = [ln for ln in lines[1:] if ln.lstrip().startswith(("-", "–", "—"))]
            if len(bullets) > N:
                bullets = bullets[:N]
            cleaned = [title] + [b if b.startswith("-") else f"- {b.lstrip('—– ')}" for b in bullets]
            text = "\n".join(cleaned).strip()
        return (text, links)
    except Exception:
        # Fallback: regelbaserad text
        from .news import render_news
        return render_news(players, lang=lang, target_sec=target_sec, ctx=ctx)
