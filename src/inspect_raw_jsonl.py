"""
Inspect raw Jetstream JSONL.
"""

import argparse
from collections import Counter
from pathlib import Path

import orjson


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect Jetstream JSONL.")
    parser.add_argument("--in-jsonl", required=True, help="Input JSONL file path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    in_path = Path(args.in_jsonl)
    total = 0
    kind_counts = Counter()
    collection_counts = Counter()
    post_substring = 0
    with in_path.open("rb") as f:
        for line in f:
            total += 1
            if b"app.bsky.feed.post" in line:
                post_substring += 1
            line = line.strip()
            if not line:
                continue
            try:
                event = orjson.loads(line)
            except orjson.JSONDecodeError:
                kind_counts["invalid_json"] += 1
                continue
            kind = event.get("kind") or "unknown"
            kind_counts[str(kind)] += 1
            if kind == "commit":
                commit = event.get("commit") or {}
                collection = commit.get("collection") or "unknown"
                collection_counts[str(collection)] += 1
    top_collections = collection_counts.most_common(10)
    print(f"lines={total}")
    print(f"kind_counts={dict(kind_counts)}")
    if top_collections:
        print(f"commit_collections_top10={top_collections}")
    else:
        print("commit_collections_top10=[]")
    print(f"lines_with_post_substring={post_substring}")


if __name__ == "__main__":
    main()
