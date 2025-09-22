import json
from pathlib import Path
from src.sections.gpt import run_gpt

def build_section(args):
    """
    Builds a duo expert conversation section based on provided news items and two personas.
    """
    news_path = Path(args.news[0])
    with open(news_path, "r", encoding="utf-8") as f:
        news_items = [json.loads(line) for line in f]

    if not news_items:
        return {"section": "S.OPINION.DUO_EXPERTS", "content": ""}

    # Use only the first news item for now
    item = news_items[0]

    persona_ids = getattr(args, "persona_ids", "").split(",")
    persona_data = {}
    if persona_ids and getattr(args, "personas", None):
        with open(args.personas, "r", encoding="utf-8") as f:
            personas = json.load(f)
        for pid in persona_ids:
            if pid in personas:
                persona_data[pid] = personas[pid]

    prompt = f"""
    SYSTEM_RULES:
    You are writing a football podcast segment with two expert commentators.
    Output MUST be in English, ~160–200 words total.
    Alternate between the two personas naturally (dialogue format).
    Fold in news facts without inventing specifics.
    No placeholders like [TEAM].
    Keep it record-ready and natural.
    End with a joint takeaway line.

    Personas:
    {json.dumps(persona_data, indent=2)}

    News:
    {json.dumps(item, indent=2)}
    """

    dialogue = run_gpt(prompt)

    return {
        "section": "S.OPINION.DUO_EXPERTS",
        "personas": persona_ids,
        "content": dialogue.strip(),
    }
