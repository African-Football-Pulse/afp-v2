import argparse
import importlib
import yaml
import json
import sys
from pathlib import Path

def build_section(section_name, args, library):
    if "sections" not in library or section_name not in library["sections"]:
        raise RuntimeError(f"Section {section_name} not found in library")

    cfg = library["sections"][section_name]
    mod_name = cfg["module"]
    runner = cfg.get("runner", "build_section")

    mod_path = f"src.sections.{mod_name}"
    try:
        mod = importlib.import_module(mod_path)
    except Exception as e:
        raise RuntimeError(f"Failed to import module {mod_path}: {e}")

    fn = getattr(mod, runner, None)
    if fn is None:
        raise RuntimeError(f"Module {mod_path} missing function {runner}")

    return fn(args)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--section", required=True, help="Section name (e.g. NEWS.TOP3)")
    parser.add_argument("--date", required=True, help="Date for the section")
    parser.add_argument("--path-scope", default="local")
    parser.add_argument("--league", default="premier_league")
    parser.add_argument("--outdir", default="sections")
    parser.add_argument("--write-latest", action="store_true")
    parser.add_argument("--news", nargs="*")
    parser.add_argument("--personas")
    parser.add_argument("--persona-id")
    parser.add_argument("--persona-ids")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    library_path = "config/sections_library.yaml"
    with open(library_path, "r", encoding="utf-8") as f:
        library = yaml.safe_load(f)

    section_obj = build_section(args.section, args, library)

    # Output path
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / f"{args.section}_{args.date}.json"

    if not args.dry_run:
        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(section_obj, f, ensure_ascii=False, indent=2)
        print(f"[produce_section] Wrote {outfile}")
    else:
        print(json.dumps(section_obj, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
