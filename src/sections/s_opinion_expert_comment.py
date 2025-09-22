import json
from pathlib import Path
from src.sections.gpt import run_gpt

def build_section(args):
    """
    Builds an expert comment section based on provided news items and persona.
    """
    # Load news items
    news_path = Path(args.news[0])
    with open(news_path, "r", encoding="utf-8") as f:
        news_items = [json.loads(line) for line in f]

    if not news_items:
        return {"section": "S.OPINION.EXPERT_COMMENT", "content": ""}

    # Use only the first news item for now
    item = news_items[0]

    # Persona loading
    persona_id = getattr(args, "persona_id", None)
    persona_data = None
    if persona_id and getattr(args, "personas", None):
        with open(args.personas, "r", encoding="utf-8") as f:
            personas = json.load(f)
        persona_data = personas.get(persona_id)

    prompt = f"""
    SYSTEM_RULES:
    You are an expert scriptwriter for a football podcast.
    Output MUST be in English, ~120–160 words.
    Stay in character based on the persona block.
    Fold in news facts without inventing specifics.
    No placeholders like [TEAM].
    Avoid list formats; produce a flowing monologue.
    End with a crisp takeaway.

    Persona:
    {json.dumps(persona_data, indent=2) if persona_data else "N/A"}

    News:
    {json.dumps(item, indent=2)}
    """

    comment = run_gpt(prompt)

    return {
        "section": "S.OPINION.EXPERT_COMMENT",
        "persona": persona_id,
        "content": comment.strip(),
    }
