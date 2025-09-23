def get_latest_finished_date(manifest) -> str | None:
    """
    Hitta senaste matchdatum i manifest som ligger innan dagens datum.
    Stödjer både { "matches": [...] } och en lista direkt.
    Returnerar datumsträng 'YYYY-MM-DD' eller None.
    """
    if not manifest:
        return None

    if isinstance(manifest, dict):
        matches = manifest.get("matches", [])
    elif isinstance(manifest, list):
        matches = manifest
    else:
        return None

    today = datetime.utcnow().date()
    dates = []

    for m in matches:
        if isinstance(m, dict):
            raw_date = m.get("match_date") or m.get("date")
            if not raw_date:
                continue
            try:
                # Hantera både YYYY-MM-DD och DD/MM/YYYY
                if "-" in raw_date:
                    dt = datetime.strptime(raw_date, "%Y-%m-%d").date()
                else:
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
