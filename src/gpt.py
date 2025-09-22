"""
GPT rendering wrapper for AFP Producer.
Centralizes all interaction with OpenAI models.

Public API:
    render_gpt(prompt_config, ctx, system_rules) -> str
        Main entrypoint for generating text from GPT.

    run_gpt = render_gpt
        Backwards-compatible alias for older modules.

Expected inputs:
    - prompt_config: dict
        Configuration block for this GPT call (e.g. persona, constraints).
    - ctx: dict
        Context data (e.g. news items, candidates, metadata).
    - system_rules: str
        System-level instructions (role, hard constraints).

Output:
    - str: Generated text (ready-to-record monologue, dialogue, etc.)
"""

import os
import logging
from openai import OpenAI
from typing import Dict, Any

# --------------------------------------------------------------------
# Logging setup
# --------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[gpt] %(message)s")

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
def _assemble_messages(prompt_config: Dict[str, Any], ctx: Dict[str, Any], system_rules: str):
    """
    Build chat messages for GPT call.
    """
    persona_block = prompt_config.get("persona", "")
    extra_instructions = prompt_config.get("instructions", "")

    # Merge persona with news/context
    user_prompt = []
    if persona_block:
        user_prompt.append(f"[Persona]\n{persona_block}")
    if ctx:
        user_prompt.append(f"[Context]\n{ctx}")
    if extra_instructions:
        user_prompt.append(f"[Instructions]\n{extra_instructions}")

    return [
        {"role": "system", "content": system_rules},
        {"role": "user", "content": "\n\n".join(user_prompt)},
    ]


# --------------------------------------------------------------------
# Main entrypoint
# --------------------------------------------------------------------
def render_gpt(prompt_config: Dict[str, Any], ctx: Dict[str, Any], system_rules: str) -> str:
    """
    Generate GPT output for given config, context, and system rules.
    """
    client = _get_client()
    messages = _assemble_messages(prompt_config, ctx, system_rules)

    logger.info("Calling GPT with persona=%s", prompt_config.get("persona_id", "N/A"))

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


# --------------------------------------------------------------------
# Backwards compatibility
# --------------------------------------------------------------------
# Older modules expect run_gpt(...)
run_gpt = render_gpt
