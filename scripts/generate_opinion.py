#!/usr/bin/env python3
import os, json, argparse, datetime, re, time
from datetime import datetime, UTC
import urllib.parse as up
import requests
from pathlib import Path

# ---- OpenAI SDK v1 ----
from openai import OpenAI

def read_text(p: Path) -> str:
    if not p.exists():
        raise SystemExit(f"Input not found: {p}")
    return p.read_text(encoding="utf-8").strip()

def load_personas(personas_path: Path):
    data = json.loads(personas_path.read_text(encoding="utf-8"))
    assert "AK" in data and "JJK" in data, "personas.json must contain AK and JJK keys"
    return data

SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a single speaker monologue that lasts ~45 seconds (≈120–160 words).
Hard constraints:
- Output MUST be in English.
- Stay strictly in character based on the provided persona block.
- Fold in news facts without inventing specifics.
- No placeholders like [TEAM]; use only info present in the news input.
- Avoid list formats; deliver a flowing, spoken monologue.
- Keep it record-ready: natural pacing, light rhetorical devices, 1–2 short pauses (…).
- End with a crisp takeaway line.

Return JSON with:
{
  "speaker": "<AK or JJK>",
  "words": <int>,
  "duration_sec": <int>,   // ~ words / 2.6
  "text": "<monologue>",
  "safety_notes": "<empty or minimal>"
}
"""

USER_TEMPLATE = """PERSONA:
{name} — {role}
Voice: {voice}
Tone: primary={tone_primary}; secondary={tone_secondary}; micro={tone_micro}
Style: {style}
Catchphrases: {catchphrases}
Traits: {traits}

NEWS_INPUT:
{news}

Guidance:
- Length target: 130–150 words (~45s).
- Reflect the persona’s tone and likely opinions.
- Use at most one catchphrase (if it fits naturally).
"""

def approx_duration(words: int) -> int:
    # ~2.6 words/sec spoken pace
    return max(30, min(60, int(round(words / 2.6))))

def clamp_words(text: str, min_w=120, max_w=170):
    # Clamp to target range without abrupt cutoffs
    words = text.split()
    if len(words) > max_w:
        text = " ".join(words[:max_w])
        text = re.sub(r"[,;:–-]\s*\S*$", ".", text)
        if not text.endswith((".", "!", "?")):
            text += "."
    return text

def make_blob_url(container_sas_url: str, blob_path: str) -> str:
    """Join container SAS URL with a blob path."""
    p = up.urlparse(container_sas_url)
    base = f"{p.scheme}://{p.netloc}{p.path.rstrip('/')}"
    return f"{base}/{blob_path.lstrip('/')}?{p.query}"

def upload_bytes_via_sas(container_sas_url: str, blob_path: str, data: bytes,
                          content_type: str = "application/octet-stream",
                          retries: int = 3, backoff: float = 0.8) -> str:
    """Upload using plain HTTP PUT + SAS (no SDK), with simple retries."""
    blob_url = make_blob_url(container_sas_url, blob_path)
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "x-ms-version": "2020-10-02",
        "Content-Type": content_type,
    }
    for attempt in range(1, retries + 1):
        r = requests.put(blob_url, headers=headers, data=data, timeout=60)
        if r.status_code in (201, 202):
            return blob_url
        if attempt == retries:
            raise RuntimeError(f"Blob upload failed ({r.status_code}): {r.text[:500]}")
        time.sleep(backoff * attempt)

def main():
    ap = argparse.ArgumentParser(description="Generate ~45s expert opinion (AK/JJK) from news input.")
    ap.add_argument("--speaker", choices=["AK", "JJK"], required=True, help="Persona key")
    ap.add_argument("--news", required=True, help="Path to news .txt/.md")
    ap.add_argument("--personas", default="config/personas.json", help="Path to personas.json")
    ap.add_argument("--outdir", default="outputs/sections", help="Output directory (fallback if no SAS)")
    ap.add_argument("--model", default=os.getenv("OPENAI_MODEL", "gpt-4o-mini"), help="OpenAI model name")
    ap.add_argument("--dry-run", action="store_true", help="Print blob URLs instead of uploading")
    # Producer-friendly path controls
    ap.add_argument("--date", default=datetime.now(UTC).strftime("%Y-%m-%d"),
                  help="ISO date folder, default=UTC today")
    ap.add_argument("--league", default="_", help="League/competition key, e.g. 'premier_league'")
    ap.add_argument("--topic", default="_", help="Optional topic subfolder, e.g. 'arsenal'")
    ap.add_argument("--layout", choices=["alias-first","date-first"], default="alias-first",
                    help="Blob path layout strategy")
    ap.add_argument("--write-latest", action="store_true",
                    help="Also write sections/<SECTION_CODE>/latest.json pointer")
    args = ap.parse_args()

    api_key = os.getenv("AFP_OPENAI_SECRETKEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("Missing API key. Set AFP_OPENAI_SECRETKEY or OPENAI_API_KEY.")

    client = OpenAI(api_key=api_key)

    personas = load_personas(Path(args.personas))
    p = personas[args.speaker]

    news_input = read_text(Path(args.news))
    user_prompt = USER_TEMPLATE.format(
        name=p["name"],
        role=p["role"],
        voice=p.get("voice",""),
        tone_primary=p.get("tone",{}).get("primary",""),
        tone_secondary=p.get("tone",{}).get("secondary",""),
        tone_micro=p.get("tone",{}).get("micro",""),
        style=p.get("style",""),
        catchphrases=", ".join(p.get("catchphrases", [])[:3]),
        traits=", ".join(p.get("traits", [])[:6]),
        news=news_input
    )

    resp = client.chat.completions.create(
        model=args.model,
        temperature=0.6,
        response_format={"type": "json_object"},
        messages=[
            {"role":"system","content": SYSTEM_RULES},
            {"role":"user","content": user_prompt},
        ],
    )

    data = json.loads(resp.choices[0].message.content)
    text = clamp_words(data.get("text",""))
    word_count = len(text.split())
    data["text"] = text
    data["words"] = word_count
    data["duration_sec"] = approx_duration(word_count)
    data["speaker"] = args.speaker

    # ---- OUTPUT (producer-style) ----
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    section_code = "S.OPINION.EXPERT_COMMENT"

    # Build bytes
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    md_bytes = (
        f"### Opinion — {data['speaker']} ({data['duration_sec']}s / {data['words']} words)\n\n"
        f"{data['text']}\n"
    ).encode("utf-8")

    # Layout
    if args.layout == "alias-first":
        base_path = f"sections/{section_code}/{args.date}/{args.league}/{args.topic}"
    else:  # date-first
        base_path = f"sections/{args.date}/{args.league}/{args.topic}/{section_code}"

    json_rel = f"{base_path}/section.json"
    md_rel   = f"{base_path}/section.md"
    man_rel  = f"{base_path}/section_manifest.json"

    # Minimal manifest for assemble
    manifest = {
        "section_code": section_code,
        "speaker": args.speaker,
        "model": args.model,
        "created_utc": ts,
        "league": args.league,
        "topic": args.topic,
        "date": args.date,
        "blobs": {
            "json": json_rel,
            "md": md_rel
        },
        "metrics": {
            "words": data["words"],
            "duration_sec": data["duration_sec"]
        },
        "sources": {
            "news_input_path": str(Path(args.news))
        }
    }
    man_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    container_sas = os.getenv("BLOB_CONTAINER_SAS_URL") or os.getenv("AFP_AZURE_SAS_URL")

    if container_sas:
        if args.dry_run:
            print("=== DRY RUN ===")
            print("Would upload JSON →", make_blob_url(container_sas, json_rel))
            print("Would upload MD   →", make_blob_url(container_sas, md_rel))
            print("Would upload MAN  →", make_blob_url(container_sas, man_rel))
            if args.write_latest:
                latest_rel = f"sections/{section_code}/latest.json"
                print("Would upload LATEST →", make_blob_url(container_sas, latest_rel))
        else:
            j_url = upload_bytes_via_sas(container_sas, json_rel, json_bytes, "application/json")
            m_url = upload_bytes_via_sas(container_sas, md_rel, md_bytes, "text/markdown")
            mf_url = upload_bytes_via_sas(container_sas, man_rel, man_bytes, "application/json")
            print(f"Uploaded JSON → {j_url}")
            print(f"Uploaded MD   → {m_url}")
            print(f"Uploaded MAN  → {mf_url}")

            if args.write_latest:
                latest_rel = f"sections/{section_code}/latest.json"
                latest_doc = {
                    "date": args.date,
                    "league": args.league,
                    "topic": args.topic,
                    "manifest": man_rel,
                    "json": json_rel,
                    "md": md_rel,
                    "created_utc": ts
                }
                latest_bytes = json.dumps(latest_doc, ensure_ascii=False, indent=2).encode("utf-8")
                latest_url = upload_bytes_via_sas(container_sas, latest_rel, latest_bytes, "application/json")
                print(f"Uploaded LATEST → {latest_url}")
    else:
        # Fallback: write locally in same structure
        outdir = Path(args.outdir) / base_path
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "section.json").write_text(json_bytes.decode("utf-8"), encoding="utf-8")
        (outdir / "section.md").write_text(md_bytes.decode("utf-8"), encoding="utf-8")
        (outdir / "section_manifest.json").write_text(man_bytes.decode("utf-8"), encoding="utf-8")
        if args.write_latest:
            latest = {
                "date": args.date,
                "league": args.league,
                "topic": args.topic,
                "manifest": str(outdir / "section_manifest.json"),
                "json": str(outdir / "section.json"),
                "md": str(outdir / "section.md"),
                "created_utc": ts
            }
            (Path(args.outdir) / "sections" / "S.OPINION.EXPERT_COMMENT" / "latest.json").parent.mkdir(parents=True, exist_ok=True)
            (Path(args.outdir) / "sections" / "S.OPINION.EXPERT_COMMENT" / "latest.json").write_text(
                json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        print(f"Wrote local: {outdir / 'section.json'}")
        print(f"Wrote local: {outdir / 'section.md'}")
        print(f"Wrote local: {outdir / 'section_manifest.json'}")

if __name__ == "__main__":
    main()
