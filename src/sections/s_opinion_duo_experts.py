import json
from src.gpt import run_gpt

SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a conversation between TWO experts that lasts ~60–70 seconds (≈160–200 words).
Hard constraints:
- Output MUST be in English.
- Stay strictly in character based on the provided persona blocks.
- Fold in news facts without inventing specifics.
- No placeholders like [PLAYER]; use only info present in the news input.
- Alternate turns naturally, max 2–3 exchanges each.
- Keep it record-ready: natural pacing, light rhetorical devices, a few pauses (…).
- End with a crisp joint takeaway line.
"""

def build_section(args):
    # use path directly
    news_path = args.news[0]

    with open(news_path, "r", encoding="utf-8") as f:
        news_items = [json.loads(line) for line in f]

    if not news_items:
        return {
            "section": args.section,
            "content": "No news items available for duo experts."
        }

    # take top 2–3 news items
    top_items = news_items[:3]

    news_text = "\n".join([item.get("title", "") for item in top_items])

    prompt = f"""Personas:
{args.personas}

News input:
{news_text}

Write the expert duo dialogue now:"""

    output = run_gpt(SYSTEM_RULES, prompt)

    return {
        "section": args.section,
        "personas": args.persona_ids,
        "content": output.strip(),
    }
