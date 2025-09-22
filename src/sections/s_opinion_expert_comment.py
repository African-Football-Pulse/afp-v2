import json
from src.gpt import run_gpt

SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a single speaker monologue that lasts ~45 seconds (≈120–160 words).
Hard constraints:
- Output MUST be in English.
- Stay strictly in character based on the provided persona block.
- Fold in news facts without inventing specifics.
- No placeholders like [TEAM]; use only info present in the news input.
- Avoid list formats; deliver a flowing, spoken monologue.
- Keep it record-ready: natural pacing, light rhetorical devices, 1–2 short pauses (…).
- End with a crisp takeaway line.
"""

def build_section(args):
    # use path directly
    news_path = args.news[0]

    with open(news_path, "r", encoding="utf-8") as f:
        news_items = [json.loads(line) for line in f]

    if not news_items:
        return {
            "section": args.section,
            "content": "No news items available for expert comment."
        }

    # take top 1–2 news items
    top_items = news_items[:2]

    news_text = "\n".join([item.get("title", "") for item in top_items])

    prompt = f"""Persona:
{args.persona}

News input:
{news_text}

Write the expert monologue now:"""

    output = run_gpt(SYSTEM_RULES, prompt)

    return {
        "section": args.section,
        "persona": args.persona_id,
        "content": output.strip(),
    }
