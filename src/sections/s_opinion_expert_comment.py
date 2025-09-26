# src/sections/s_opinion_duo_experts.py
import yaml
from src.sections import utils
from src.sections.utils import write_outputs
from src.producer.gpt import run_gpt


def load_roles():
    with open("config/speaking_roles.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_section(args):
    """Produce a duo-expert opinion section with GPT commentary."""

    day = args.date
    league = args.league or "_"
    pod = getattr(args, "pod", "default")
    lang = getattr(args, "lang", "en")  # e.g. "en", "sw", "ar"
    section_code = getattr(args, "section", "S.OPINION.DUO_EXPERTS")

    # Load candidates
    candidates = utils.load_scored_enriched(day)
    if not candidates:
        print(f"[opinion_duo] WARN: No candidates available for {day}")
        payload = {
            "slug": "opinion_duo_experts",
            "title": "Duo Expert Opinion",
            "text": "No candidates available.",
            "length_s": 2,
            "sources": {},
            "meta": {"persona": "system"},
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

    # Load role mapping
    roles = load_roles()
    role_cfg = roles.get("roles", {}).get("duo_experts", {}).get(lang, {})
    expert1 = role_cfg.get("expert1", "expert1")
    expert2 = role_cfg.get("expert2", "expert2")

    # Persona block (pods.yaml)
    persona_id, persona_block = utils.get_persona_block("expert_duo", pod)

    # GPT setup
    instructions = (
        f"Write a lively exchange (~140 words total) in {lang} between two African football experts. "
        f"Use clear speaker labels like '{expert1}:' and '{expert2}:' so the dialogue can be voiced by two different personas. "
        f"Base the discussion on these stories:\n\n{news_text}\n\n"
        f"Make it conversational, record-ready, and avoid lists or placeholders."
    )
    prompt_config = {
        "persona": persona_block,
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
        "meta": {
            "persona": persona_id,
            "expert1": expert1,
            "expert2": expert2,
        },
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
