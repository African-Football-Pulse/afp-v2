import yaml, pathlib, json

CONFIG_PATH = pathlib.Path("config/pods.yaml")

def main():
    if not CONFIG_PATH.exists():
        raise RuntimeError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    pods = cfg.get("pods", {})

    active = {k: v for k, v in pods.items() if v.get("status") == "on"}
    inactive = {k: v for k, v in pods.items() if v.get("status") != "on"}

    print("✅ Active pods:")
    print(json.dumps(active, indent=2, ensure_ascii=False))

    print("\nℹ️ Inactive pods:")
    print(json.dumps(inactive, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
