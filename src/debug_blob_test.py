import os
from src.storage import azure_blob

def main():
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
    blob_path = "sections/debug_test/testfile.txt"
    test_text = "Hello from debug_blob_test!"

    print(f"[debug] Container = {container}")
    print(f"[debug] Försöker skriva till {blob_path}")

    try:
        azure_blob.put_text(container, blob_path, test_text)
        print(f"[debug] ✅ Lyckades skriva till {blob_path}")
    except Exception as e:
        print(f"[debug] ❌ Fel vid skrivning: {e}")
        return

    try:
        content = azure_blob.get_text(container, blob_path)
        print(f"[debug] ✅ Läste tillbaka: {content}")
    except Exception as e:
        print(f"[debug] ❌ Fel vid läsning: {e}")

    try:
        files = azure_blob.list_prefix(container, "sections/debug_test")
        print(f"[debug] ✅ list_prefix hittade: {files}")
    except Exception as e:
        print(f"[debug] ❌ Fel vid listning: {e}")

if __name__ == "__main__":
    main()
