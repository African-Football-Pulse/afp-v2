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
        if isinstance(m, dict) and "date" in m:
            try:
                dt = datetime.strptime(m["date"], "%d/%m/%Y").date()
                if dt < today:
                    dates.append(dt)
            except Exception:
                continue

    if not dates:
        print("[debug] Inga giltiga datum hittades i manifest")
        return None

    latest = max(dates)
    print(f"[debug] Valde senaste matchdatum: {latest}")
    return latest.strftime("%Y-%m-%d")
