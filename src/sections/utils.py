def write_outputs(
    section_code: str,
    *args,
    league: str = None,
    lang: str = "en",
    day: str = None,
    payload: Dict = None,
    path_scope: str = "blob",
    status: str = "ok"
):
    """
    Standardized output writer for sections.
    Supports both legacy positional calls and keyword calls.
    """

    # Legacy positional handling
    # Possible orders we've seen:
    #   write_outputs("CODE", day, league, payload, ...)
    #   write_outputs("CODE", league, lang, day, payload, ...)
    if args:
        if len(args) == 3:  # (day, league, payload)
            day, league, payload = args
        elif len(args) == 4:  # (league, lang, day, payload)
            league, lang, day, payload = args
        else:
            raise ValueError(f"[write_outputs] Unexpected positional args: {args}")

    if not all([section_code, day, league, payload]):
        raise ValueError("[write_outputs] Missing required arguments")

    base_path = f"sections/{section_code}/{day}/{league}/_/"

    # Ensure status is also reflected in payload.meta
    if "meta" not in payload:
        payload["meta"] = {}
    payload["meta"]["status"] = status

    # Write section.json
    azure_blob.upload_json(CONTAINER, base_path + "section.json", payload)

    # Write section.md (if text exists)
    if "text" in payload:
        azure_blob.put_text(CONTAINER, base_path + "section.md", payload["text"])

    # Build and write manifest
    manifest = {
        "section_code": section_code,
        "league": league,
        "lang": lang,
        "day": day,
        "title": payload.get("title", ""),
        "type": payload.get("type", ""),
        "sources": payload.get("sources", {}),
        "status": status,
    }
    azure_blob.upload_json(CONTAINER, base_path + "section_manifest.json", manifest)

    print(f"[utils] Uploaded {base_path}section.json")
    if "text" in payload:
        print(f"[utils] Uploaded {base_path}section.md")
    print(f"[utils] Uploaded {base_path}section_manifest.json")

    return manifest
