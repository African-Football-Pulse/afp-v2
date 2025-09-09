import os, json, re, time
from datetime import datetime, UTC
from pathlib import Path
import urllib.parse as up
import requests
from openai import OpenAI

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
  "duration_sec": <int>,
  "text": "<monologue>"
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

def _read_text(p: Path) -> str:
    """
    Load input text. If JSON, extract headlines/summary fields.
    If plain text, return as-is.
    """
    if p.suffix.lower() == ".json":
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            # Support both list-of-items and dict{"items": [...]} formats
            if isinstance(data, dict) and "items" in data:
                items = data["items"]
            else:
                items = data
            if isinstance(items, list):
                texts = []
                for item in items[:5]:  # ta de första 5 för input
                    if isinstance(item, dict):
                        if "title" in item:
                            texts.append(item["title"])
                        elif "summary" in item:
                            texts.append(item["summary"])
                return "\n".join(texts)
        except Exception as e:
            print(f"[s_opinion_expert_comment] WARN: Failed to parse JSON {p}: {e}")
            return ""
    # fallback
    return p.read_text(encoding="utf-8").strip()

def _load_personas(p: Path):
    data = json.loads(p.read_text(encoding="utf-8"))
    assert "AK" in data and "JJK" in data, "personas.json must contain AK and JJK"
    return data

def _clamp_words(text: str, max_w=170):
    words = text.split()
    if len(words) > max_w:
        text = " ".join(words[:max_w])
        text = re.sub(r"[,;:–-]\s*\S*$", ".", text)
        if not text.endswith((".", "!", "?")):
            text += "."
    return text

def _approx_duration(words: int) -> int:
    return max(30, min(60, int(round(words / 2.6))))  # ~2.6 w/s

# ---- Azure Blob via SAS (no SDK) ----
def _make_blob_url(container_sas_url: str, blob_path: str) -> str:
    p = up.urlparse(container_sas_url)
    base = f"{p.scheme}://{p.netloc}{p.path.rstrip('/')}"
    return f"{base}/{blob_path.lstrip('/')}?{p.query}"

def _upload_bytes(container_sas_url: str, blob_path: str, data: bytes,
                  content_type="application/octet-stream", retries=3, backoff=0.8) -> str:
    url = _make_blob_url(container_sas_url, blob_path)
    headers = {"x-ms-blob-type": "BlockBlob", "x-ms-version": "2020-10-02", "Content-Type": content_type}
    for attempt in range(1, retries+1):
        r = requests.put(url, headers=headers, data=data, timeout=60)
        if r.status_code in (201, 202):
            return url
        if attempt == retries:
            raise RuntimeError(f"Blob upload failed ({r.status_code}): {r.text[:500]}")
        time.sleep(backoff * attempt)

def build_section(
    *,
    section_code: str,
    news_path: str,
    personas_path: str,
    date: str,
    league: str = "_",
    topic: str = "_",
    speaker: str | None = None,
    layout: str = "alias-first",
    path_scope: str = "single",
    write_latest: bool = True,
    dry_run: bool = False,
    outdir: str = "outputs/sections",
    model: str = "gpt-4o-mini",
    type: str = "opinion",
) -> dict:

    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    personas = _load_personas(Path(personas_path))
    if speaker is None:
        speaker = "JJK"
    p = personas[speaker]
    news_input = _read_text(Path(news_path))

    api_key = os.getenv("AFP_OPENAI_SECRETKEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("Missing API key. Set AFP_OPENAI_SECRETKEY or OPENAI_API_KEY.")
    client = OpenAI(api_key=api_key)

    user_prompt = USER_TEMPLATE.format(
        name=p["name"], role=p["role"],
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
        model=model, temperature=0.6, response_format={"type": "json_object"},
        messages=[{"role":"system","content": SYSTEM_RULES},
                  {"role":"user","content": user_prompt}]
    )
    data = json.loads(resp.choices[0].message.content)
    text = _clamp_words(data.get("text",""))
    words = len(text.split())
    data.update({"text": text, "words": words, "duration_sec": _approx_duration(words), "speaker": speaker})

    persona_part = (speaker if path_scope == "speaker" else "")
    if layout == "alias-first":
        base = f"sections/{section_code}/{date}/{league}/{topic}"
    else:
        base = f"sections/{date}/{league}/{topic}/{section_code}"
    if persona_part:
        base = f"{base}/{persona_part}"

    json_rel = f"{base}/section.json"
    md_rel   = f"{base}/section.md"
    man_rel  = f"{base}/section_manifest.json"

    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    md_bytes = (f"### Opinion — {speaker} ({data['duration_sec']}s / {data['words']} words)\n\n{text}\n").encode("utf-8")
    manifest = {
        "section_code": section_code,
        "type": type,
        "speaker": speaker,
        "model": model,
        "created_utc": ts,
        "league": league,
        "topic": topic,
        "date": date,
        "blobs": { "json": json_rel, "md": md_rel },
        "metrics": { "words": data["words"], "duration_sec": data["duration_sec"] },
        "sources": { "news_input_path": str(news_path), "personas_path": str(personas_path) }
    }
    man_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    sas = os.getenv("BLOB_CONTAINER_SAS_URL") or os.getenv("AFP_AZURE_SAS_URL")
    if sas:
        if dry_run:
            print("=== DRY RUN ===")
            print("Would upload JSON →", _make_blob_url(sas, json_rel))
            print("Would upload MD   →", _make_blob_url(sas, md_rel))
            print("Would upload MAN  →", _make_blob_url(sas, man_rel))
        else:
            _upload_bytes(sas, json_rel, json_bytes, "application/json")
            _upload_bytes(sas, md_rel, md_bytes, "text/markdown")
            _upload_bytes(sas, man_rel, man_bytes, "application/json")
    else:
        outdirp = Path(outdir) / base
        outdirp.mkdir(parents=True, exist_ok=True)
        (outdirp / "section.json").write_text(json_bytes.decode("utf-8"), encoding="utf-8")
        (outdirp / "section.md").write_text(md_bytes.decode("utf-8"), encoding="utf-8")
        (outdirp / "section_manifest.json").write_text(man_bytes.decode("utf-8"), encoding="utf-8")

    return manifest
