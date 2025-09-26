# src/sections/s_opinion_duo_experts.py
import yaml
from src.sections import utils
from src.sections.utils import write_outputs
from src.producer.gpt import run_gpt


def load_duo_experts(lang: str):
    """Load expert1 and expert2 personas for given language from speaking_roles.yaml."""
    with open("config/speaking_roles.yaml", "r", encoding="utf-8") as f:
        roles_cfg = yaml.safe_load(f)

    duo_cfg = roles_cfg.get("roles", {}).get("duo_experts", {})
    if lang not in duo_cfg:
        raise ValueError(f"[duo_experts] No duo_experts defined for lang={lang}")

    return duo_cfg[lang]["expert1"], duo_cfg[lang]["expert2"]


def build_section(args):
    """Produce a duo-expert opinion section with GPT commentary."""

    day = args.date
    league = args.league or "_"
    pod = getattr(args, "pod", "default")
    lang = getattr(args, "lang", "en")
    section_code = getattr(args, "section", "S.OPINION.DUO.EXPERTS")

    # Load scored_enriched candidates
    candidates = utils.load_scored_enriched(day)
    if not candidates:
        print(f"[opinion_duo] WARN: No candidates available for {day}")
        payload = {
            "slug": "opinion_duo_experts",
            "title": "Duo Expert Opinion",
            "text": "No candidates available.",
            "length_s": 2,
            "sources": {},
            "meta": {"personas": []},
            "type": "opinion",
            "model": "static",
            "items": [],
        }
        return write_outputs(
            section_code=section_code,
            day=day,
            league=league,
            payload=payload,
            status="no_candidates",
            lang=lang,
        )

    # Pick two candidates
    first_item = candidates[0]
    second_item = candidates[1] if len(candidates) > 1 else candidates[0]
    news_text = f"- {first_item.get('text','')}\n- {second_item.get('text','')}"

    # Personas (expert duo)
    expert1, expert2 = load_duo_experts(lang)

    # GPT setup
    instructions = (
        f"Create a lively two-voice exchange (~140 words total) in {lang}, "
        f"between {expert1.upper()} and {expert2.upper()}, "
        f"based on these stories:\n\n{news_text}\n\n"
        f"Make it conversational, record-ready, and avoid lists or placeholders."
    )
    prompt_config = {
        "persona": f"duo_experts:{expert1}+{expert2}",
        "instructions": instructions,
    }
    ctx = {"candidates": [first_item, second_item]}
    system_rules = "You are an assistant generating a natural spoken-style football dialogue."

    gpt_output = run_gpt(prompt_config, ctx, system_rules)

    payload = {
        "slug": "opinion_duo_experts",
        "title": "Duo Expert Opinion",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {},
        "meta": {"personas": [expert1, expert2]},
        "type": "opinion",
        "model": "gpt",
        "items": [first_item, second_item],
    }

    return write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        payload=payload,
        status="ok",
        lang=lang,
    )
