def get_latest_finished_date(manifest) -> str | None:
    """
    Hitta senaste matchdatum i manifest som ligger innan dagens datum.
    Stödjer SoccerData-struktur med leagues -> stages -> matches.
    Returnerar datumsträng 'YYYY-MM-DD' eller None.
    """
    if not manifest:
        return None

    today = datetime.utcnow().date()
    dates = []

    # Manifest kan vara list eller dict
    leagues = manifest if isinstance(manifest, list) else [manifest]

    for league in leagues:
        for stage in league.get("stage", []):
            for m in stage.get("matches", []):
                raw_date = m.get("date")
                if not raw_date:
                    continue
                try:
                    dt = datetime.strptime(raw_date, "%d/%m/%Y").date()
                    if dt < today:
                        dates.append(dt)
                except Exception:
                    continue

    if not dates:
        print("[collectors] ⚠️ Inga matchdatum före idag hittades i manifest")
        return None

    latest = max(dates)
    print(f"[collectors] ✅ Valde senaste matchdatum: {latest}")
    return latest.strftime("%Y-%m-%d")
