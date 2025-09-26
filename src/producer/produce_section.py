# src/producer/produce_section.py
import argparse
import importlib
import yaml
import json
import sys
from pathlib import Path

from src.producer import role_utils


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
    parser.add_argument("--pod", help="Pod key from pods.yaml")
    args = parser.parse_args()

    # Library
    library_path = "src/producer/sections_library.yaml"
    with open(library_path, "r", encoding="utf-8") as f:
        library = yaml.safe_load(f)

    section_obj = build_section(args.section, args, library)

    # Acceptera två typer av manifest
    if isinstance(section_obj, dict):
        # Normal sektion
        if "section_code" in section_obj:
            print(f"[produce_section] Received manifest from {args.section}")

            # Role-mapping
            cfg = library["sections"][args.section]
            role = cfg.get("role")
            if role:
                pods_cfg = role_utils.load_yaml("config/pods.yaml")["pods"]
                if args.pod and args.pod in pods_cfg:
                    persona_id = role_utils.resolve_persona_for_role(pods_cfg[args.pod], role)
                    section_obj.setdefault("meta", {})["persona"] = persona_id
                    print(f"[produce_section] Role '{role}' mapped to persona '{persona_id}'")
                else:
                    print(f"[produce_section] No pod specified or not found, skipping role mapping")

            if args.dry_run:
                print(json.dumps(section_obj, ensure_ascii=False, indent=2))

        # Driver-sektion
        elif "manifest" in section_obj:
            print(f"[produce_section] Received DRIVER manifest from {args.section}")
            if args.dry_run:
                print(json.dumps(section_obj, ensure_ascii=False, indent=2))

        else:
            raise RuntimeError(
                f"Section {args.section} did not return a valid manifest. Keys: {list(section_obj.keys())}"
            )
    else:
        raise RuntimeError(
            f"Section {args.section} did not return a manifest. Got: {type(section_obj)}"
        )


if __name__ == "__main__":
    main()
