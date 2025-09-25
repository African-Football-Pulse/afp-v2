📌 Förklaring per steg

Collect (rss_multi)

Hämtar RSS-feeds.

Skriver Raw:
raw/news/<feed>/<day>/rss.json (metadata).

Extraherar artiklar → Curated:
curated/news/<feed>/<league>/<day>/items.json.

Produce Candidates (produce_candidates)

Läser Curated items.json.

Matchar mot masterlistan (afrikanska spelare).

Skriver Producer:
producer/candidates/<day>/candidates.jsonl.

Produce Scoring (produce_scoring)

Läser Producer candidates.jsonl.

Viktar (recency, novelty, importance).

Skriver Producer:
producer/scored/<day>/scored.jsonl.

Produce Section (produce_section / produce_auto)

Läser Producer scored.jsonl.

Bygger sektion (ex. S.NEWS.TOP3).

Skriver Sections:
sections/S.NEWS.TOP3/<day>/<league>/{section.md, section.json, section_manifest.json}.
