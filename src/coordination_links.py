"""
Coordination detection via co-sharing domains in time windows.
"""

import argparse
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from urllib.parse import urlparse

import networkx as nx
import orjson
import pandas as pd

from .paths import ARTIFACTS_DIR, ensure_dirs


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect coordination via shared link domains.")
    parser.add_argument("--in-parquet", required=True, help="Input posts parquet.")
    parser.add_argument("--window-minutes", type=int, default=10, help="Window size in minutes.")
    parser.add_argument("--min-domain-posts", type=int, default=5, help="Minimum posts per domain.")
    parser.add_argument("--max-bucket-accounts", type=int, default=200, help="Max accounts per bucket.")
    parser.add_argument("--out-prefix", default=None, help="Output prefix for artifacts.")
    return parser.parse_args()


def extract_domains(urls) -> list[str]:
    if not isinstance(urls, list):
        return []
    domains = []
    for url in urls:
        if not isinstance(url, str):
            continue
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        if not netloc:
            continue
        domains.append(netloc)
    if not domains:
        return []
    seen = set()
    out = []
    for domain in domains:
        if domain not in seen:
            seen.add(domain)
            out.append(domain)
    return out


def main() -> None:
    args = parse_args()
    ensure_dirs()
    in_path = Path(args.in_parquet)
    if args.out_prefix:
        out_prefix = Path(args.out_prefix)
    else:
        out_prefix = ARTIFACTS_DIR / f"coordination_{utc_stamp()}"
    posts = pd.read_parquet(in_path)
    if posts.empty:
        print("No posts available for coordination detection.")
        return
    required = {"time_us", "did_hash", "urls"}
    if not required.issubset(posts.columns):
        print("Missing required columns for coordination detection.")
        return
    posts = posts.reset_index(drop=True)
    n_posts_with_urls = int(posts["urls"].apply(lambda x: isinstance(x, list) and len(x) > 0).sum())
    expanded = []
    for post_id, (time_us, did_hash, urls) in enumerate(
        posts[["time_us", "did_hash", "urls"]].itertuples(index=False, name=None)
    ):
        if urls is None:
            continue
        if isinstance(urls, str):
            urls = [urls]
        elif isinstance(urls, (list, tuple)):
            urls = list(urls)
        else:
            continue
        if not urls:
            continue
        domains = extract_domains(urls)
        if not domains:
            continue
        for domain in domains:
            expanded.append({"post_id": post_id, "time_us": time_us, "did_hash": did_hash, "domain": domain})
    if not expanded:
        print("No link domains available for coordination detection.")
        return
    expanded_df = pd.DataFrame(expanded)
    domain_post_counts = expanded_df.groupby("domain")["post_id"].nunique()
    eligible_domains = domain_post_counts[domain_post_counts >= args.min_domain_posts].index
    expanded_df = expanded_df[expanded_df["domain"].isin(eligible_domains)].copy()
    if expanded_df.empty:
        summary = {
            "n_posts_with_urls": n_posts_with_urls,
            "n_domains_after_filter": 0,
            "n_edges": 0,
            "n_clusters": 0,
            "skipped_buckets": 0,
        }
        Path(f"{out_prefix}_summary.json").write_bytes(orjson.dumps(summary))
        print("No eligible domains after filtering.")
        return
    n_domains_after_filter = int(expanded_df["domain"].nunique())
    window_us = args.window_minutes * 60 * 1_000_000
    expanded_df["bucket_id"] = expanded_df["time_us"].apply(
        lambda x: int(math.floor(int(x) / window_us)) if pd.notna(x) else 0
    )
    edge_weights = Counter()
    skipped_buckets = 0
    domain_buckets_used = Counter()
    domain_buckets_skipped = Counter()
    bucket_records = []
    grouped = expanded_df.groupby(["domain", "bucket_id"])
    for (domain, bucket_id), group in grouped:
        accounts = sorted(set(group["did_hash"].dropna().astype(str).tolist()))
        if len(accounts) == 0:
            continue
        if len(accounts) > args.max_bucket_accounts:
            skipped_buckets += 1
            domain_buckets_skipped[domain] += 1
            continue
        domain_buckets_used[domain] += 1
        bucket_records.append((domain, accounts))
        for a, b in combinations(accounts, 2):
            edge_weights[(a, b)] += 1
    if not edge_weights:
        summary = {
            "n_posts_with_urls": n_posts_with_urls,
            "n_domains_after_filter": n_domains_after_filter,
            "n_edges": 0,
            "n_clusters": 0,
            "skipped_buckets": skipped_buckets,
        }
        Path(f"{out_prefix}_summary.json").write_bytes(orjson.dumps(summary))
        print("No coordination edges found.")
        return
    edges_rows = [{"src": a, "dst": b, "weight": int(w)} for (a, b), w in edge_weights.items()]
    edges_df = pd.DataFrame(edges_rows)
    out_edges = Path(f"{out_prefix}_edges.parquet")
    edges_df.to_parquet(out_edges, index=False)
    domain_rows = []
    for domain in eligible_domains:
        domain_rows.append(
            {
                "domain": domain,
                "posts": int(domain_post_counts.get(domain, 0)),
                "buckets_used": int(domain_buckets_used.get(domain, 0)),
                "buckets_skipped": int(domain_buckets_skipped.get(domain, 0)),
            }
        )
    out_domains = Path(f"{out_prefix}_domains.csv")
    pd.DataFrame(domain_rows).to_csv(out_domains, index=False)
    graph = nx.Graph()
    for row in edges_rows:
        graph.add_edge(row["src"], row["dst"], weight=row["weight"])
    components = list(nx.connected_components(graph))
    cluster_id_map = {}
    for idx, comp in enumerate(components):
        for did_hash in comp:
            cluster_id_map[did_hash] = idx
    cluster_domain_counts = defaultdict(Counter)
    for domain, accounts in bucket_records:
        for did_hash in accounts:
            cluster_id = cluster_id_map.get(did_hash)
            if cluster_id is not None:
                cluster_domain_counts[cluster_id][domain] += 1
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
        domain_counts = cluster_domain_counts.get(cluster_id, Counter())
        total_domain_events = sum(domain_counts.values())
        if domain_counts:
            top_domain, top_count = domain_counts.most_common(1)[0]
            top_share = (top_count / total_domain_events) if total_domain_events else 0.0
        else:
            top_domain = ""
            top_share = 0.0
        cluster_rows.append(
            {
                "cluster_id": cluster_id,
                "n_accounts": int(n_accounts),
                "total_edge_weight": int(total_w),
                "top_domain": top_domain,
                "top_domain_share": float(top_share),
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
        "n_posts_with_urls": n_posts_with_urls,
        "n_domains_after_filter": n_domains_after_filter,
        "n_edges": int(len(edges_rows)),
        "n_clusters": int(len(components)),
        "skipped_buckets": int(skipped_buckets),
    }
    Path(f"{out_prefix}_summary.json").write_bytes(orjson.dumps(summary))
    print(
        "coordination_ready "
        f"clusters={summary['n_clusters']} "
        f"edges={summary['n_edges']} "
        f"window_min={args.window_minutes}"
    )


if __name__ == "__main__":
    main()
