import sys
from src.publisher import build_publish_request, buzzsprout_simple

def main():
    print("🚀 Start publish_auto")
    try:
        # 1. Bygg publish_request.json i Azure
        print("▶️ Kör build_publish_request")
        build_publish_request.main()

        # 2. Publicera till Buzzsprout
        print("▶️ Kör buzzsprout_simple")
        buzzsprout_simple.main()

        print("✅ publish_auto klart")
    except Exception as e:
        print(f"❌ publish_auto misslyckades: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
