"""
Jetstream ingestion and post extraction.
"""

import argparse
import asyncio
import hashlib
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

import orjson
import pandas as pd
import websockets

from .paths import PROCESSED_DIR, RAW_DIR, ensure_dirs

POST_COLLECTION = "app.bsky.feed.post"
URL_PATTERN = re.compile(r"https?://\\S+")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def extract_did_from_uri(uri: str) -> Optional[str]:
    if not uri:
        return None
    if uri.startswith("at://"):
        parts = uri.split("/")
        if len(parts) >= 3:
            return parts[2]
    return None


def dedupe_preserve(items: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


RAW_URL_PATTERN = re.compile(r"https?://[^\\s\\\"\\}\\]]+")


def extract_urls_from_any(obj) -> List[str]:
    urls = []

    def visit(value) -> None:
        if len(urls) >= 20:
            return
        if isinstance(value, str):
            if value.startswith("http://") or value.startswith("https://"):
                urls.append(value)
            return
        if isinstance(value, dict):
            for item in value.values():
                visit(item)
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                visit(item)
            return

    visit(obj)
    urls = [u for u in urls if isinstance(u, str) and u]
    return dedupe_preserve(urls)[:20]


def extract_urls_from_raw_line(raw_line_bytes: bytes) -> List[str]:
    try:
        text = raw_line_bytes.decode("utf-8", "replace")
    except Exception:
        text = ""
    if not text:
        return []
    urls = RAW_URL_PATTERN.findall(text)
    urls = [u for u in urls if isinstance(u, str) and u]
    return dedupe_preserve(urls)[:20]


def build_url(base_url: str, wanted_posts: bool) -> str:
    if not wanted_posts:
        return base_url
    parsed = urlsplit(base_url)
    query = parsed.query
    extra = urlencode({"wantedCollections": POST_COLLECTION})
    if query:
        query = f"{query}&{extra}"
    else:
        query = extra
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


async def capture_events(
    url: str,
    raw_path: Path,
    minutes: int,
    max_events: Optional[int],
) -> int:
    end_time = None
    if max_events is None:
        end_time = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    count = 0
    async with websockets.connect(url) as ws:
        with open(raw_path, "wb") as f:
            while True:
                if max_events is not None and count >= max_events:
                    break
                if end_time is not None and datetime.now(timezone.utc) >= end_time:
                    break
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if message is None:
                    continue
                if isinstance(message, (bytes, bytearray)):
                    msg_b = bytes(message)
                else:
                    msg_b = str(message).encode("utf-8", "replace")
                while msg_b.endswith(b"\x0A") or msg_b.endswith(b"\x0D"):
                    msg_b = msg_b[:-1]
                f.write(msg_b)
                f.write(b"\x0A")
                count += 1
    return count


def extract_posts(raw_path: Path, parquet_path: Path) -> int:
    rows = []
    posts_with_urls = 0
    urls_total = 0
    with raw_path.open("rb") as f:
        for line in f:
            try:
                event = orjson.loads(line)
            except orjson.JSONDecodeError:
                continue
            if event.get("kind") != "commit":
                continue
            commit = event.get("commit")
            if not isinstance(commit, dict):
                continue
            collection = commit.get("collection")
            operation = commit.get("operation")
            record = commit.get("record")
            if collection != POST_COLLECTION or operation not in {"create", "update"}:
                continue
            if not isinstance(record, dict):
                continue
            did = event.get("did") or event.get("repo")
            if not did:
                continue
            text = record.get("text", "")
            if not isinstance(text, str):
                text = ""
            time_us = event.get("time_us")
            try:
                time_us = int(time_us)
            except (TypeError, ValueError):
                time_us = 0
            did_hash = sha256_hex(did)
            reply = record.get("reply") or {}
            parent = reply.get("parent") or {}
            parent_did = parent.get("did") or extract_did_from_uri(parent.get("uri", ""))
            reply_parent_did_hash = sha256_hex(parent_did) if parent_did else None
            mentions = []
            facets = record.get("facets")
            if isinstance(facets, list):
                for facet in facets:
                    features = facet.get("features") or []
                    for feature in features:
                        if isinstance(feature, dict):
                            did_val = feature.get("did")
                            handle_val = feature.get("handle")
                            if isinstance(did_val, str) and did_val:
                                mentions.append(did_val)
                            if isinstance(handle_val, str) and handle_val:
                                mentions.append(handle_val)
            mentions = dedupe_preserve(mentions) if mentions else []
            urls_a = extract_urls_from_any(record)
            urls_b = extract_urls_from_raw_line(line)
            urls = dedupe_preserve(urls_a + urls_b)[:20]
            if urls:
                posts_with_urls += 1
                urls_total += len(urls)
            lang = None
            if isinstance(record.get("langs"), list) and record.get("langs"):
                lang = record.get("langs")[0]
            rows.append(
                {
                    "time_us": time_us,
                    "did_hash": did_hash,
                    "text": text,
                    "reply_parent_did_hash": reply_parent_did_hash,
                    "mentions": mentions,
                    "urls": urls,
                    "lang": lang,
                }
            )
    columns = [
        "time_us",
        "did_hash",
        "text",
        "reply_parent_did_hash",
        "mentions",
        "urls",
        "lang",
    ]
    if rows:
        df = pd.DataFrame(rows, columns=columns)
    else:
        df = pd.DataFrame(columns=columns)
    df.to_parquet(parquet_path, index=False)
    print(f"posts_with_urls={posts_with_urls} urls_total={urls_total}")
    return len(df)


def debug_commit_stats(raw_path: Path, limit: int = 5000) -> tuple[list, list]:
    collection_counts = Counter()
    operation_counts = Counter()
    with raw_path.open("rb") as f:
        for _ in range(limit):
            line = f.readline()
            if not line:
                break
            try:
                event = orjson.loads(line)
            except orjson.JSONDecodeError:
                continue
            if event.get("kind") != "commit":
                continue
            commit = event.get("commit")
            if not isinstance(commit, dict):
                continue
            collection = commit.get("collection")
            operation = commit.get("operation")
            if collection is not None:
                collection_counts[str(collection)] += 1
            if operation is not None:
                operation_counts[str(operation)] += 1
    return collection_counts.most_common(5), operation_counts.most_common(5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Bluesky Jetstream and extract posts.")
    parser.add_argument(
        "--url",
        default="wss://jetstream2.us-west.bsky.network/subscribe",
        help="Jetstream WebSocket endpoint.",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=5,
        help="Capture duration in minutes when max-events is not set.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after capturing this many events.",
    )
    parser.add_argument(
        "--no-post-filter",
        action="store_true",
        help="Disable wantedCollections=app.bsky.feed.post filtering.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()
    stamp = utc_stamp()
    raw_path = RAW_DIR / f"jetstream_{stamp}.jsonl"
    parquet_path = PROCESSED_DIR / f"posts_{stamp}.parquet"
    url = build_url(args.url, not args.no_post_filter)
    mode = f"max-events={args.max_events}" if args.max_events else f"minutes={args.minutes}"
    print(f"Starting Jetstream capture ({mode}) -> {raw_path}")
    event_count = asyncio.run(capture_events(url, raw_path, args.minutes, args.max_events))
    post_count = extract_posts(raw_path, parquet_path)
    print(
        "Capture complete. "
        f"events={event_count} posts={post_count} "
        f"raw={raw_path} parquet={parquet_path}"
    )
    line_count = 0
    with open(raw_path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            line_count += chunk.count(b"\x0A")
    print(f"raw_lines_written={line_count}")
    print(f"raw_lines_expected={event_count}")
    if post_count == 0:
        top_collections, top_operations = debug_commit_stats(raw_path)
        print(f"commit_collections_top5={top_collections}")
        print(f"commit_operations_top5={top_operations}")


if __name__ == "__main__":
    main()
