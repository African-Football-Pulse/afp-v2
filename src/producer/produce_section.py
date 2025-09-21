# src/producer/produce_section.py
import argparse
import importlib
import json
import os
import yaml
from src.storage import azure_blob

def build_section(section_code, args, library):
    entry = library.get(section_code)
    if not entry:
        raise RuntimeError(f"Sektionskod {section_code} saknas i library")

    mod_path = entry.get("module")
    runner = entry.get("runner", "build_section")

    try:
        mod = importlib.import_module(mod_path)
    except Exception as e:
        raise RuntimeError(f"Kunde inte importera modul {mod_path}: {e}")

    fn = getattr(mod, runner, None)
    if fn is None:
        raise RuntimeError(f"Modulen {mod_path} saknar funktionen {runner}")

    print(f"[produce_section] Running {section_code} via {mod_path}.{runner}")
    if args.persona_id:
        print(f"[produce_section] Persona-ID: {args.persona_id}")
    if args.persona_ids:
        print(f"[produce_section] Persona-IDs: {args.persona_ids}")

    return fn(args)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--section-code", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--path-scope", default="blob")
    parser.add_argument("--league", default="premier_league")
    parser.add_argument("--outdir", default="sections")
    parser.add_argument("--write-latest", action="store_true")
    parser.add_argument("--news", nargs="*")
    parser.add_argument("--dry-run", action="store_true")

    # Nya argument för opinion-sektioner
    parser.add_argument("--personas", default="config/personas.json")
    parser.add_argument("--persona-id", default=None)
    parser.add_argument("--persona-ids", default=None)

    args = parser.parse_args()

    # FIX: rätt sökväg
    library_path = os.getenv("SECTIONS_LIBRARY", "src/producer/sections_library.yaml")
    with open(library_path, "r", encoding="utf-8") as f:
        library = yaml.safe_load(f)

    section_obj = build_section(args.section_code, args, library)

    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f"{args.section_code}_{args.date}.json")
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(section_obj, f, ensure_ascii=False, indent=2)

    print(f"[produce_section] Wrote {outpath}")

if __name__ == "__main__":
    main()
