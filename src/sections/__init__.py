# src/sections/__init__.py
import os
import sys

# Importera builders som redan finns i din mapp
from .s_news_top3_generic import main as build_news_topn
from .s_news_top3_guardian import main as build_guardian_top3

SECTION_BUILDERS = {
    "S.NEWS.TOPN": build_news_topn,            # generiska Top-N (över flera feeds)
    "S.NEWS.TOP3_GUARDIAN": build_guardian_top3,  # legacy/canary
}

def run():
    section_id = os.getenv("SECTION_ID", "S.NEWS.TOP3_GUARDIAN")
    builder = SECTION_BUILDERS.get(section_id)
    if not builder:
        print(f"Unknown SECTION_ID: {section_id}", file=sys.stderr)
        sys.exit(2)
    builder()

if __name__ == "__main__":
    run()
