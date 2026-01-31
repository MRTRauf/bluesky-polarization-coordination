"""
Coordination detection via shared reply targets in time windows.
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path

import networkx as nx
import orjson
import pandas as pd

from .paths import ARTIFACTS_DIR, ensure_dirs


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect coordination via shared reply targets.")
    parser.add_argument("--in-parquet", required=True, help="Input posts parquet.")
    parser.add_argument("--window-minutes", type=int, default=10, help="Window size in minutes.")
    parser.add_argument("--min-target-posts", type=int, default=10, help="Minimum buckets per target.")
    parser.add_argument("--max-bucket-accounts", type=int, default=200, help="Max accounts per bucket.")
    parser.add_argument("--out-prefix", default=None, help="Output prefix for artifacts.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()
    in_path = Path(args.in_parquet)
    if args.out_prefix:
        out_prefix = Path(args.out_prefix)
    else:
        out_prefix = ARTIFACTS_DIR / f"coordtargets_{utc_stamp()}"
    posts = pd.read_parquet(in_path)
    n_posts = int(len(posts))
    if posts.empty:
        print("No posts available for coordination targets.")
        return
    required = {"time_us", "did_hash", "reply_parent_did_hash"}
    if not required.issubset(posts.columns):
        print("Missing required columns for coordination targets.")
        return
    reply_df = posts[posts["reply_parent_did_hash"].notna()].copy()
    n_reply_rows = int(len(reply_df))
    if reply_df.empty:
        print("No reply rows available for coordination targets.")
        return
    window_us = args.window_minutes * 60 * 1_000_000
    reply_df["bucket_id"] = reply_df["time_us"].apply(lambda x: int(x) // window_us)
    reply_df["target_id"] = reply_df["reply_parent_did_hash"].astype(str)
    target_events = Counter()
    skipped_buckets = 0
    grouped = reply_df.groupby(["target_id", "bucket_id"])
    bucket_accounts = {}
    for (target_id, bucket_id), group in grouped:
        accounts = sorted(set(group["did_hash"].dropna().astype(str).tolist()))
        if len(accounts) < 2:
            continue
        if len(accounts) > args.max_bucket_accounts:
            skipped_buckets += 1
            continue
        target_events[target_id] += 1
        bucket_accounts[(target_id, bucket_id)] = accounts
    n_targets_total = int(len(target_events))
    kept_targets = {t for t, c in target_events.items() if c >= args.min_target_posts}
    n_targets_kept = int(len(kept_targets))
    if not kept_targets:
        summary = {
            "n_posts": n_posts,
            "n_reply_rows": n_reply_rows,
            "n_targets_total": n_targets_total,
            "n_targets_kept": n_targets_kept,
            "n_edges": 0,
            "n_clusters": 0,
            "skipped_buckets": int(skipped_buckets),
            "window_minutes": args.window_minutes,
            "min_target_posts": args.min_target_posts,
        }
        Path(f"{out_prefix}_summary.json").write_bytes(orjson.dumps(summary))
        print("No targets meet min_target_posts.")
        return
    edge_weights = Counter()
    target_buckets_used = Counter()
    for (target_id, bucket_id), accounts in bucket_accounts.items():
        if target_id not in kept_targets:
            continue
        target_buckets_used[target_id] += 1
        for a, b in combinations(accounts, 2):
            edge_weights[(a, b)] += 1
    if not edge_weights:
        summary = {
            "n_posts": n_posts,
            "n_reply_rows": n_reply_rows,
            "n_targets_total": n_targets_total,
            "n_targets_kept": n_targets_kept,
            "n_edges": 0,
            "n_clusters": 0,
            "skipped_buckets": int(skipped_buckets),
            "window_minutes": args.window_minutes,
            "min_target_posts": args.min_target_posts,
        }
        Path(f"{out_prefix}_summary.json").write_bytes(orjson.dumps(summary))
        print("No coordination edges found after filtering.")
        return
    edges_rows = [{"src": a, "dst": b, "weight": int(w)} for (a, b), w in edge_weights.items()]
    edges_df = pd.DataFrame(edges_rows)
    out_edges = Path(f"{out_prefix}_edges.parquet")
    edges_df.to_parquet(out_edges, index=False)
    targets_rows = [{"target_id": t, "buckets_used": int(c)} for t, c in target_buckets_used.items()]
    out_targets = Path(f"{out_prefix}_targets.csv")
    pd.DataFrame(targets_rows).to_csv(out_targets, index=False)
    graph = nx.Graph()
    for row in edges_rows:
        graph.add_edge(row["src"], row["dst"], weight=row["weight"])
    components = list(nx.connected_components(graph))
    cluster_id_map = {}
    for idx, comp in enumerate(components):
        for did_hash in comp:
            cluster_id_map[did_hash] = idx
    cluster_target_counts = defaultdict(Counter)
    for (target_id, bucket_id), accounts in bucket_accounts.items():
        if target_id not in kept_targets:
            continue
        for did_hash in accounts:
            cluster_id = cluster_id_map.get(did_hash)
            if cluster_id is not None:
                cluster_target_counts[cluster_id][target_id] += 1
    cluster_edge_weight = Counter()
    account_scores = Counter()
    for (a, b), w in edge_weights.items():
        account_scores[a] += w
        account_scores[b] += w
        if cluster_id_map.get(a) == cluster_id_map.get(b):
            cluster_edge_weight[cluster_id_map[a]] += w
    cluster_rows = []
    for cluster_id, accounts in enumerate(components):
        n_accounts = len(accounts)
        total_w = int(cluster_edge_weight.get(cluster_id, 0))
        target_counts = cluster_target_counts.get(cluster_id, Counter())
        total_target_events = sum(target_counts.values())
        if target_counts:
            top_target, top_count = target_counts.most_common(1)[0]
            top_share = (top_count / total_target_events) if total_target_events else 0.0
        else:
            top_target = ""
            top_share = 0.0
        cluster_rows.append(
            {
                "cluster_id": cluster_id,
                "n_accounts": int(n_accounts),
                "total_edge_weight": int(total_w),
                "top_target": top_target,
                "top_target_share": float(top_share),
            }
        )
    out_clusters = Path(f"{out_prefix}_clusters.csv")
    pd.DataFrame(cluster_rows).to_csv(out_clusters, index=False)
    account_rows = [
        {"did_hash": did_hash, "cluster_id": cluster_id_map[did_hash], "coord_score": int(score)}
        for did_hash, score in account_scores.items()
    ]
    out_accounts = Path(f"{out_prefix}_accounts.csv")
    pd.DataFrame(account_rows).to_csv(out_accounts, index=False)
    summary = {
        "n_posts": n_posts,
        "n_reply_rows": n_reply_rows,
        "n_targets_total": n_targets_total,
        "n_targets_kept": n_targets_kept,
        "n_edges": int(len(edges_rows)),
        "n_clusters": int(len(components)),
        "skipped_buckets": int(skipped_buckets),
        "window_minutes": args.window_minutes,
        "min_target_posts": args.min_target_posts,
    }
    Path(f"{out_prefix}_summary.json").write_bytes(orjson.dumps(summary))
    print(
        "coordtargets_ready "
        f"clusters={summary['n_clusters']} "
        f"edges={summary['n_edges']} "
        f"window_min={args.window_minutes} "
        f"min_target_posts={args.min_target_posts}"
    )


if __name__ == "__main__":
    main()
