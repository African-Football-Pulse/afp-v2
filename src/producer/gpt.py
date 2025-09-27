"""
Minimal GPT wrapper for AFP Producer.
Clean logging: only our own lines with [gpt] prefix.
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from openai import OpenAI

# -------------------------------------------------------
# Setup clean logger just for this module
# -------------------------------------------------------
logger = logging.getLogger("gpt")
handler = logging.StreamHandler()
formatter = logging.Formatter("[gpt] %(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler]
logger.setLevel(logging.INFO)
logger.propagate = False   # 🚨 Viktigt: stoppa propagation till root

# Silence noisy libs
for noisy in [
    "azure",
    "azure.storage",
    "azure.core.pipeline.policies.http_logging_policy",
    "httpx",
    "openai",
]:
    logging.getLogger(noisy).setLevel(logging.CRITICAL)

# -------------------------------------------------------
# OpenAI client singleton
# -------------------------------------------------------
_client = None
def _get_client() -> OpenAI:
    global _client
    if _client is None:
        api_key = os.getenv("AFP_OPENAI_SECRETKEY")
        if not api_key:
            raise RuntimeError("Missing environment variable: AFP_OPENAI_SECRETKEY")
        _client = OpenAI(api_key=api_key)
    return _client

# -------------------------------------------------------
# Build messages
# -------------------------------------------------------
def _assemble_messages(prompt_config: Dict[str, Any], ctx: Optional[Any], system_rules: Optional[str]):
    persona_block = prompt_config.get("persona") or "news_anchor"
    extra_instructions = prompt_config.get("instructions") or ""

    user_prompt = []
    if persona_block:
        user_prompt.append(f"[Persona]\n{persona_block}")
    if ctx:
        try:
            ctx_str = json.dumps(ctx, ensure_ascii=False, indent=2)
        except Exception:
            ctx_str = str(ctx)
        user_prompt.append(f"[Context]\n{ctx_str}")
    if extra_instructions:
        user_prompt.append(f"[Instructions]\n{extra_instructions}")

    safe_system = system_rules if system_rules and isinstance(system_rules, str) else "You are a helpful assistant."

    return [
        {"role": "system", "content": safe_system},
        {"role": "user", "content": "\n\n".join(user_prompt)},
    ]

# -------------------------------------------------------
# Main entrypoint
# -------------------------------------------------------
def render_gpt(prompt_config: Dict[str, Any], ctx: Optional[Any], system_rules: Optional[str] = None) -> str:
    client = _get_client()
    messages = _assemble_messages(prompt_config, ctx, system_rules)

    persona = prompt_config.get("persona", "news_anchor")
    logger.info("Calling GPT with persona=%s", persona)

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7,
            max_tokens=500,
        )
        text = resp.choices[0].message.content.strip()
        logger.info("Generated text length=%d chars", len(text))
        return text
    except Exception as e:
        logger.error("GPT call failed: %s", e)
        raise

# Backwards compatibility
run_gpt = render_gpt
