# src/assemble/assemble_episode.py
import argparse, json, os, pathlib, sys, yaml

def load_yaml(p):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def read_section_text(base, date, section_code):
    path = pathlib.Path(base) / date / section_code / "manifest.json"
    if not path.exists():
        return None, str(path)  # returnera None om filen inte finns
    with open(path, "r", encoding="utf-8") as f:
        m = json.load(f)
    return m.get("payload", {}).get("text", "").strip(), str(path)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--league", required=True)
    ap.add_argument("--mode", default="postmatch")
    ap.add_argument("--lang", default="en")
    ap.add_argument("--template", default="config/episode_templates/postmatch.yaml")
    ap.add_argument("--sections_root", default="producer/sections")
    ap.add_argument("--out_root", default="assembler/episodes")
    args = ap.parse_args()

    tpl = load_yaml(args.template)["episode"]
    segs = tpl["segments"]

    lines = []
    manifest_segments = []

    for s in segs:
        if s["type"] == "section":
            text, src_path = read_section_text(args.sections_root, args.date, s["section_code"])
            if text:
                persona = s.get("persona", "AK")
                lines.append(f"[{persona}] {text}")
                manifest_segments.append({
                    "type": "section",
                    "section_code": s["section_code"],
                    "persona": s.get("persona", "AK"),
                    "source": src_path
                })
            else:
                # markera som missing
                manifest_segments.append({
                    "type": "section",
                    "section_code": s["section_code"],
                    "persona": s.get("persona", "AK"),
                    "source": src_path,
                    "missing": True
                })
                print(f"[WARN] Missing section source: {src_path}")
        else:
            manifest_segments.append(s)

    out_dir = pathlib.Path(args.out_root) / args.date / args.league / args.mode / args.lang
    out_dir.mkdir(parents=True, exist_ok=True)

    episode_manifest = {
        "episode_id": f"{args.date}-{args.league}-{args.mode}-{args.lang}",
        "date": args.date,
        "league": args.league,
        "mode": args.mode,
        "lang": args.lang,
        "title": tpl.get("title", "AFP Episode"),
        "segments": manifest_segments,
        "audio": {
            "target_duration_s": 420,
            "tts": {"engine": "elevenlabs", "default_voice": "AK"}
        }
    }

    (out_dir / "episode_manifest.json").write_text(
        json.dumps(episode_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    (out_dir / "episode_script.txt").write_text(
        "\n\n".join(lines),
        encoding="utf-8"
    )

    print(f"✅ Done → {out_dir}")

if __name__ == "__main__":
    sys.exit(main())
