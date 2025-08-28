#!/usr/bin/env python3
import os, json, sys, argparse, datetime, re
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
    # If needed, trim gently to stay within target band
    words = text.split()
    if len(words) > max_w:
        text = " ".join(words[:max_w])
        # finish sentence nicely
        text = re.sub(r"[,;:–-]\s*\S*$", ".", text)
        if not text.endswith((".", "!", "?")):
            text += "."
    return text

def make_blob_url(container_sas_url: str, blob_path: str) -> str:
    """Join container SAS URL with a blob path."""
    p = up.urlparse(container_sas_url)
    base = f"{p.scheme}://{p.netloc}{p.path.rstrip('/')}"
    return f"{base}/{blob_path.lstrip('/')}?{p.query}"

def upload_bytes_via_sas(container_sas_url: str, blob_path: str, data: bytes, content_type: str = "application/octet-stream"):
    """Upload using plain HTTP PUT + SAS (no SDK)."""
    blob_url = make_blob_url(container_sas_url, blob_path)
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "x-ms-version": "2020-10-02",
        "Content-Type": content_type,
    }
    r = requests.put(blob_url, headers=headers, data=data, timeout=60)
    if r.status_code not in (201, 202):
        raise RuntimeError(f"Blob upload failed ({r.status_code}): {r.text[:300]}")
    return blob_url


def main():
    ap = argparse.ArgumentParser(description="Generate ~45s expert opinion (AK/JJK) from news input.")
    ap.add_argument("--speaker", choices=["AK", "JJK"], required=True, help="Persona key")
    ap.add_argument("--news", required=True, help="Path to news .txt/.md")
    ap.add_argument("--personas", default="config/personas.json", help="Path to personas.json")
    ap.add_argument("--outdir", default="outputs/sections", help="Output directory")
    ap.add_argument("--model", default=os.getenv("OPENAI_MODEL", "gpt-4o-mini"), help="OpenAI model name")
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

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = f"opinion_{args.speaker}_{ts}"

    # JSON
    (outdir / f"{stem}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    # MD (ready-to-read)
    md = f"""### Opinion — {data['speaker']} ({data['duration_sec']}s / {data['words']} words)

{data['text']}
    # ---- OUTPUT ----
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = f"opinion_{args.speaker}_{ts}"

    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    md_bytes = (
        f"### Opinion — {data['speaker']} ({data['duration_sec']}s / {data['words']} words)\n\n"
        f"{data['text']}\n"
    ).encode("utf-8")

    # SAS från Secrets / env
    container_sas = os.getenv("BLOB_CONTAINER_SAS_URL") or os.getenv("AFP_AZURE_SAS_URL")

    if container_sas:
        # Upload to Azure Blob
        json_path = f"sections/{stem}.json"
        md_path   = f"sections/{stem}.md"

        json_url = upload_bytes_via_sas(container_sas, json_path, json_bytes, "application/json")
        md_url   = upload_bytes_via_sas(container_sas, md_path, md_bytes, "text/markdown")

        print(f"Uploaded JSON → {json_url}")
        print(f"Uploaded MD   → {md_url}")
    else:
        # Fallback: skriv lokalt
        outdir = Path(args.outdir)
        outdir.mkdir(parents=True, exist_ok=True)

        (outdir / f"{stem}.json").write_text(json_bytes.decode("utf-8"), encoding="utf-8")
        (outdir / f"{stem}.md").write_text(md_bytes.decode("utf-8"), encoding="utf-8")

        print(f"Wrote local: {outdir / (stem + '.json')}")
        print(f"Wrote local: {outdir / (stem + '.md')}")

if __name__ == "__main__":
    main()
