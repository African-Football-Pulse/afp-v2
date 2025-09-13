import argparse
from src.collectors.collect_match_details import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, required=True)
    parser.add_argument("--manifest", type=str, required=True, help="Blob path to weekly manifest.json")
    parser.add_argument("--with_api", action="store_true")
    args = parser.parse_args()

    run(args.league_id, args.manifest, args.with_api)
