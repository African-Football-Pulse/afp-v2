# src/sections/s_opinion_duo_experts.py
import os, json, re, time
from datetime import datetime, UTC
from pathlib import Path
import urllib.parse as up
import requests
from openai import OpenAI

SYSTEM_RULES = """You are scripting a two-expert football podcast segment.
Goal: a lively, authentic AK ↔ JJK dialogue grounded ONLY in the news input.
Constraints:
- Output MUST be in English.
- Stay strictly in character for each persona (AK = anchor/journalist; JJK = veteran coach-analyst).
- No invented facts beyond the NEWS_INPUT.
- Natural, broadcast-ready speech. No bullet lists. Keep it flowing, with brief pauses (…) used sparingly.
- AK opens, JJK replies. 2–3 total turns (AK→JJK or AK→JJK→AK). Prefer 2 turns for ~90 seconds total.
- Length target: ~90 seconds total (≈230–280 words across both speakers).
- Close with a crisp takeaway from JJK if only 2 turns, or from AK if 3 turns.

Return JSON ONLY with this structure:
{
  "dialogue": [
    {"speaker": "AK",  "text": "<AK line>"},
    {"speaker": "JJK", "text": "<JJK line>"},
    {"speaker": "AK",  "text": "<optional AK close>"}
  ]
}
"""

USER_TEMPLATE = """PERSONA A (AK):
{name_ak} — {role_ak}
Voice: {voice_ak}
Tone: primary={tone_ak_primary}; secondary={tone_ak_secondary}; micro={tone_ak_micro}
Style: {style_ak}
Catchphrases: {catch_ak}
Traits: {traits_ak}

PERSONA B (JJK):
{name_jjk} — {role_jjk}
Voice: {voice_jjk}
Tone: primary={tone_jjk_primary}; secondary={tone_jjk_secondary}; micro={tone_jjk_micro}
Style: {style_jjk}
Catchphrases: {catch_jjk}
Traits: {traits_jjk}

NEWS_INPUT:
{news}

Guidance:
- AK asks sharp, data-aware questions; frames stakes. Use at most one subtle catchphrase.
- JJK answers with anecdotes, tactical insight, mild provocation; may use one catchphrase.
- Aim for 2 turns (AK→JJK). If the topic benefits from a short anchor wrap, add AK close as a 3rd turn.
- Keep total ≈230–280 words.
"""

def _read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8").strip()

def _load_personas(p: Path):
    data = json.loads(p.read_text(encoding="utf-8"))
    assert "AK" in data and "JJK" in data, "personas.json must contain AK and JJK"
    return data

def _clamp_words(text: str, max_w=180):
    words = text.split()
    if len(words) > max_w:
        text = " ".join(words[:max_w])
        text = re.sub(r"[,;:–-]\s*\S*$", ".", text)
        if not text.endswith((".", "!", "?")):
            text += "."
    return text

def _words(s: str) -> int:
    return len(s.split())

def _approx_duration(words: int) -> int:
    # ~2.6 words per second
    return max(30, min(120, int(round(words / 2.6))))

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
    section_code: str,        # e.g. "S.OPINION.DUO_EXPERTS"
    news_path: str,
    personas_path: str,
    date: str,
    league: str = "_",
    topic: str = "_",
    layout: str = "alias-first",     # or "date-first"
    write_latest: bool = True,
    dry_run: bool = False,
    outdir: str = "outputs/sections",
    model: str = "gpt-4o-mini",
    type: str = "opinion",
) -> dict:
    """
    Producer entrypoint for a two-expert dialogue (AK ↔ JJK).
    Returns manifest dict; writes blobs via SAS if present, else locally.
    """
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    personas = _load_personas(Path(personas_path))
    ak = personas["AK"]
    jjk = personas["JJK"]
    news_input = _read_text(Path(news_path))

    user_prompt = USER_TEMPLATE.format(
        name_ak=ak["name"], role_ak=ak["role"], voice_ak=ak.get("voice",""),
        tone_ak_primary=ak.get("tone",{}).get("primary",""),
        tone_ak_secondary=ak.get("tone",{}).get("secondary",""),
        tone_ak_micro=ak.get("tone",{}).get("micro",""),
        style_ak=ak.get("style",""),
        catch_ak=", ".join(ak.get("catchphrases", [])[:3]),
        traits_ak=", ".join(ak.get("traits", [])[:6]),

        name_jjk=jjk["name"], role_jjk=jjk["role"], voice_jjk=jjk.get("voice",""),
        tone_jjk_primary=jjk.get("tone",{}).get("primary",""),
        tone_jjk_secondary=jjk.get("tone",{}).get("secondary",""),
        tone_jjk_micro=jjk.get("tone",{}).get("micro",""),
        style_jjk=jjk.get("style",""),
        catch_jjk=", ".join(jjk.get("catchphrases", [])[:3]),
        traits_jjk=", ".join(jjk.get("traits", [])[:6]),

        news=news_input
    )

    api_key = os.getenv("AFP_OPENAI_SECRETKEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("Missing API key. Set AFP_OPENAI_SECRETKEY or OPENAI_API_KEY.")
    client = OpenAI(api_key=api_key)

    resp = client.chat.completions.create(
        model=model, temperature=0.6, response_format={"type": "json_object"},
        messages=[{"role":"system","content": SYSTEM_RULES},
                  {"role":"user","content": user_prompt}]
    )
    raw = json.loads(resp.choices[0].message.content)
    dialogue = raw.get("dialogue", [])

    # Sanity & trims
    out = []
    total_words = 0
    for turn in dialogue[:3]:  # max 3 turns
        spk = turn.get("speaker","").strip()
        txt = _clamp_words(turn.get("text","").strip(), max_w=180)
        if spk not in ("AK","JJK") or not txt:
            continue
        w = _words(txt)
        total_words += w
        out.append({"speaker": spk, "text": txt, "words": w, "duration_sec": _approx_duration(w)})

    # enforce 2-turn minimum (AK->JJK)
    if len(out) < 2:
        raise SystemExit("Dialogue too short. Expected at least two turns: AK then JJK.")

    duration_total = sum(t["duration_sec"] for t in out)

    # Paths
    if layout == "alias-first":
        base = f"sections/{section_code}/{date}/{league}/{topic}"
    else:
        base = f"sections/{date}/{league}/{topic}/{section_code}"

    json_rel = f"{base}/section.json"
    md_rel   = f"{base}/section.md"
    man_rel  = f"{base}/section_manifest.json"

    # Build payloads
    data = {
        "dialogue": [{"speaker": t["speaker"], "text": t["text"]} for t in out],
        "segments": [{"speaker": t["speaker"], "words": t["words"], "duration_sec": t["duration_sec"]} for t in out],
        "words_total": total_words,
        "duration_sec_total": duration_total,
        "speakers": ["AK","JJK"]
    }
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    # Render MD
    lines = [f"### Duo Opinion — AK ↔ JJK ({duration_total}s / {total_words} words)", ""]
    for t in out:
        lines.append(f"**{t['speaker']}:** {t['text']}")
        lines.append("")
    md_bytes = ("\n".join(lines)).encode("utf-8")

    # Manifest
    manifest = {
        "section_code": section_code,
        "type": type,
        "model": model,
        "created_utc": ts,
        "league": league,
        "topic": topic,
        "date": date,
        "blobs": { "json": json_rel, "md": md_rel },
        "metrics": {
            "words_total": total_words,
            "duration_sec_total": duration_total,
            "segments": [{"speaker": t["speaker"], "words": t["words"], "duration_sec": t["duration_sec"]} for t in out]
        },
        "sources": { "news_input_path": str(news_path), "personas_path": str(personas_path) }
    }
    man_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    # Write
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
