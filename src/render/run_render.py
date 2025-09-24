import os, sys, json, pathlib
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from src.storage import azure_blob

def main():
    # Env-variabler
    league = os.getenv("LEAGUE", "premier_league")
    lang = os.getenv("LANG", "en")
    date = os.getenv("EPISODE_DATE", datetime.utcnow().date().isoformat())

    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
    if not container:
        raise RuntimeError("AZURE_STORAGE_CONTAINER missing")

    # 1) Ladda sections från assembler-output
    assembler_path = f"assembler/episodes/{date}/{league}/{lang}/episode_manifest.json"
    if not azure_blob.exists(container, assembler_path):
        raise RuntimeError(f"Hittar inte episode_manifest: {assembler_path}")
    assembler_manifest = azure_blob.get_json(container, assembler_path)
    sections = assembler_manifest.get("sections", {})

    # 2) Kör Jinja-mallen
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("episode.jinja")

    ctx = dict(
        mode="used",
        league=league,
        lang=lang,
        date=date,
        weekday=datetime.fromisoformat(date).weekday(),
        sections=sections,
    )
    rendered = template.render(**ctx)

    # 3) Extrahera vilka sektioner som faktiskt användes
    used_sections = [line.strip() for line in rendered.splitlines() if line.strip()]

    # 4) Hämta metadata direkt från Jinja-variabler
    title = template.module.episode_title
    description = template.module.episode_description

    manifest = {
        "sections": used_sections,
        "title": title,
        "description": description,
        "language": lang,
        "explicit": False,
    }

    # 5) Ladda upp render_manifest.json till Azure
    blob_path = f"audio/episodes/{date}/{league}/daily/{lang}/render_manifest.json"
    azure_blob.upload_json(container, blob_path, manifest)

    print(f"✅ Skapade render_manifest.json → {blob_path}")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
