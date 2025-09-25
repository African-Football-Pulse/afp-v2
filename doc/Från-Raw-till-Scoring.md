Måste formateras i MD.

📦 Begreppen i AFP
Raw

Här hamnar exakt det som Collect hittar, utan förädling.

Exempel: RSS-feeds sparas som raw/news/<feed>/<day>/rss.json med metadata om källan.

På samma sätt för matcher och stats: raw/stats/..., raw/matches/... osv.

Alltså: Raw = ofiltrerat insamlingsresultat.

Curated

Här hamnar material som vi bearbetar i Collect för att göra det enklare att använda i nästa steg.

Exempel: från ett RSS-flöde tar vi ut själva artiklarna och skriver dem till curated/news/<feed>/<league>/<day>/items.json.

Tillhörande input_manifest.json pekar ut vilken Curated-fil som gäller för just det feedet och datumet.

Alltså: Curated = rådata förädlad till standardiserad struktur (listor av items, normaliserade fält).

Producer

Här skriver Produce-jobben (t.ex. produce_candidates, produce_scoring).

Exempel: produce_candidates läser curated news och masterlistan, och skriver resultatet till producer/candidates/<day>/candidates.jsonl.

produce_scoring tar in kandidaterna och skriver producer/scored/<day>/scored.jsonl.

Alltså: Producer = det bearbetade material som används för att bygga sektioner.

🔄 Kedjan för News

Collect (rss_multi)

Hämtar 393 artiklar från 17 feeds.

Sparar metadata i raw/news/.../rss.json.

Extraherar artiklar och skriver till curated/news/.../items.json.

Produce Candidates

Läser curated/news/.../items.json.

Matchar mot masterlistan (51 afrikanska spelare).

Skriver resultat till producer/candidates/<day>/candidates.jsonl.

Produce Scoring

Läser producer/candidates/<day>/candidates.jsonl.

Ger viktning (recency, novelty, importance).

Skriver till producer/scored/<day>/scored.jsonl.

Produce Section (t.ex. Top3 Generic)

Läser producer/scored/<day>/scored.jsonl.

Väljer ut top 3 och skriver sektionen i sections/S.NEWS.TOP3/....

📌 Slutsats

Raw = dump (vad vi hämtade).

Curated = förädlad standardstruktur (items.json).

Producer = output från produce-stegen (candidates, scored).

Sections = slutresultat som ska in i podden.
