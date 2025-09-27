"""
GPT rendering wrapper for AFP Producer.
Centralizes all interaction with OpenAI models.
"""

import os
import logging
import json
from openai import OpenAI
from typing import Dict, Any, Optional

# --------------------------------------------------------------------
# Logging setup
# --------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[gpt] %(message)s")

DEBUG = bool(os.getenv("DEBUG_GPT", False))

# --------------------------------------------------------------------
# OpenAI client
# --------------------------------------------------------------------
_client = None


def _get_client() -> OpenAI:
    """Return singleton OpenAI client using AFP_OPENAI_SECRETKEY."""
    global _client
    if _client is None:
        api_key = os.getenv("AFP_OPENAI_SECRETKEY")
        if not api_key:
            raise RuntimeError("Missing environment variable: AFP_OPENAI_SECRETKEY")
        _client = OpenAI(api_key=api_key)
    return _client


# --------------------------------------------------------------------
# Prompt assembly
# --------------------------------------------------------------------
def _assemble_messages(prompt_config: Dict[str, Any], ctx: Optional[Any], system_rules: Optional[str]):
    """
    Build chat messages for GPT call.
    Always returns valid string content for system/user messages.
    """
    persona_block = prompt_config.get("persona") or "news_anchor"
    extra_instructions = prompt_config.get("instructions") or ""

    # Convert context to string safely (compact, only if DEBUG)
    ctx_str = ""
    if ctx and DEBUG:
        if isinstance(ctx, (dict, list)):
            try:
                ctx_str = json.dumps(ctx, ensure_ascii=False, indent=2)
            except Exception:
                ctx_str = str(ctx)
        else:
            ctx_str = str(ctx)

    user_prompt = []
    if persona_block:
        user_prompt.append(f"[Persona]\n{persona_block}")
    if ctx_str:
        user_prompt.append(f"[Context]\n{ctx_str}")
    if extra_instructions:
        user_prompt.append(f"[Instructions]\n{extra_instructions}")

    safe_system = system_rules if system_rules and isinstance(system_rules, str) else "You are a helpful assistant."

    return [
        {"role": "system", "content": safe_system},
        {"role": "user", "content": "\n\n".join(user_prompt)},
    ]


# --------------------------------------------------------------------
# Main entrypoint
# --------------------------------------------------------------------
def render_gpt(prompt_config: Dict[str, Any], ctx: Optional[Any], system_rules: Optional[str] = None) -> str:
    """
    Generate GPT output for given config, context, and system rules.
    """
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
        if DEBUG:
            logger.info("Preview: %s", text[:200])
        return text
    except Exception as e:
        logger.error("GPT call failed: %s", e)
        raise


# --------------------------------------------------------------------
# Backwards compatibility
# --------------------------------------------------------------------
run_gpt = render_gpt
