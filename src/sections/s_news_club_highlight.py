# sections/s_news_club_highlight.py
def build(ctx):
    """
    Bygger en kort klubbfokus-del: 45–60 s.
    Kollar collected items för league/lang och väljer en klubb att lyfta (t.ex. flest omnämnanden senaste dygnet).
    """
    league = ctx["league"]; lang = ctx["lang"]; target = ctx.get("target_length_s", 50)
    items = load_normalized_items(ctx)  # ni har helper i collect/produce-lagret

    # enkel heuristik: räkna klubb-mentions
    club, picked = pick_best_club(items)  # returnerar (name, [items])
    text = render_club_highlight(club, picked, lang, target)  # gärna jinja eller enkel f-string

    return {
        "slug": "club_highlight",
        "title": f"{club} spotlight",
        "text": text,
        "length_s": estimate_length(text, target),
        "sources": [i["id"] for i in picked],
        "meta": {"persona": "AK"}
    }
