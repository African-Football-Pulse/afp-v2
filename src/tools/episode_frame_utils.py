"""
episode_frame_utils.py
-------------------------------------
Stub-version av tidigare frame-hanterare.

Bakgrund:
Förr användes denna modul för att lägga till EPISODE.INTRO och EPISODE.OUTRO
baserat på episode_frame.yaml. Nu sköts detta helt av sektionerna:
  - S.GENERIC.INTRO.DAILY / POSTMATCH
  - S.GENERIC.OUTRO.DAILY

Denna fil finns kvar enbart för bakåtkompatibilitet och loggning.
-------------------------------------
"""

import logging

logger = logging.getLogger("episode_frame_utils")
handler = logging.StreamHandler()
formatter = logging.Formatter("[episode_frame_utils] %(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler]
logger.setLevel(logging.INFO)
logger.propagate = False


def insert_intro_outro(sections_meta, lang):
    """
    Stub-funktion. Returnerar sektionerna oförändrade.
    """
    logger.info("Inactive — intro/outro handled by S.GENERIC sections.")
    return sections_meta
