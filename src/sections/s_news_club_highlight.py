import argparse
from datetime import datetime
from src.sections import utils
from src.producer.gpt import run_gpt
from src.producer import role_utils


def build_section(args):
    """
    Build a 'Club Highlight' news section:
    - Picks a candidate item for a club.
    - Sends context to GPT for enriched narration.
    - Writes section outputs (json, md, manifest).
    """

    day = args.date
    league = args.league
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default")

    print(f"[s_news_club_highlight] Bygger Club Highlight för {league} @ {day}")

    # Ladda kandidater (nu från scored/scored_enriched.jsonl)
    candidates = utils.load_candidates(day)
    if not candidates:
        print("[s_news_club_highlight] ❌ Inga kandidater hittades")
        return None

    # Välj första candidate för enkelhet (senare kan vi göra logik för variation)
    candidate = candidates[0]
    player = candidate.get("player", {}).get("name", "an African player")
    club = candidate.get("player", {}).get("club", "a Premier League club")
    source = candidate.get("source", "unknown")

    # Persona (via role_utils + fallback)
    role = "news_anchor"
    persona_id, persona_block = utils.get_persona_block(role, pod)
    if not persona_block or not persona_block.strip():
        persona_id, persona_block = "news_anchor", (
            "News Anchor\nRole: Default\nVoice: Neutral\n"
            "Tone: Informative\nStyle: Clear\n"
        )

    # Bygg GPT prompt
    pretty_league = league.replace("_", " ").title()
    instructions = (
        f"You are a sports news anchor. Write a flowing news script in {lang} "
        f"highlighting one key story from the {pretty_league}. "
        f"Focus on the player {player} at {club}. "
        f"Make it engaging, but concise (about 3-4 sentences). "
        f"Do not include scores, raw URLs or metadata. "
        f"Do not add generic closings like 'Stay tuned'."
    )
    if not instructions or not instructions.strip():
        instructions = f"Write a short {lang} football news script."

    prompt_config = {
        "persona": persona_block,
        "instructions": instructions,
    }

    enriched_text = run_gpt(prompt_config, candidate, system_rules=None)

    # Bygg payload
    title = f"Club Highlight – {club}"
    payload = {
        "title": title,
        "text": enriched_text,
        "type": "news",
        "sources": {"source": source},
        "meta": {
            "day": day,
            "league": league,
            "persona": persona_id,
            "player": player,
            "club": club,
        },
    }

    # Skriv output
    manifest = utils.write_outputs(
        section_code="S.NEWS.CLUB_HIGHLIGHT",
        league=league,
        lang=lang,
        day=day,
        payload=payload,
        path_scope=getattr(args, "path_scope", "blob"),
    )

    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--league", required=True)
    parser.add_argument("--pod", required=True)
    parser.add_argument("--lang", default="en")
    parser.add_argument("--path-scope", default="blob")
    args = parser.parse_args()
    build_section(args)
