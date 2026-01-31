"""
Build reply network artifacts from posts.
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import orjson
import pandas as pd

from .paths import ARTIFACTS_DIR, ensure_dirs


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build reply network from posts parquet.")
    parser.add_argument("--in-parquet", required=True, help="Input posts parquet.")
    parser.add_argument("--out-prefix", default=None, help="Output prefix for artifacts.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()
    in_path = Path(args.in_parquet)
    if args.out_prefix:
        out_prefix = Path(args.out_prefix)
    else:
        out_prefix = ARTIFACTS_DIR / f"replynet_{utc_stamp()}"
    df = pd.read_parquet(in_path)
    if df.empty:
        print("No posts available for reply network.")
        return
    reply_mask = df["reply_parent_did_hash"].notna() & (df["reply_parent_did_hash"] != "")
    edges = df.loc[reply_mask, ["did_hash", "reply_parent_did_hash"]]
    if edges.empty:
        print("No reply edges available for reply network.")
        return
    edges = (
        edges.groupby(["did_hash", "reply_parent_did_hash"])
        .size()
        .reset_index(name="weight")
        .rename(columns={"did_hash": "src", "reply_parent_did_hash": "dst"})
    )
    out_edges = Path(f"{out_prefix}_edges.parquet")
    edges.to_parquet(out_edges, index=False)
    out_weight = edges.groupby("src")["weight"].sum().reset_index(name="out_weight")
    in_weight = edges.groupby("dst")["weight"].sum().reset_index(name="in_weight")
    nodes = (
        out_weight.rename(columns={"src": "did_hash"})
        .merge(in_weight.rename(columns={"dst": "did_hash"}), on="did_hash", how="outer")
        .fillna(0)
    )
    nodes["out_weight"] = nodes["out_weight"].astype(int)
    nodes["in_weight"] = nodes["in_weight"].astype(int)
    out_nodes = Path(f"{out_prefix}_nodes.parquet")
    nodes.to_parquet(out_nodes, index=False)
    total_weight = int(edges["weight"].sum())
    top_in = (
        nodes.sort_values("in_weight", ascending=False)
        .head(10)[["did_hash", "in_weight"]]
        .to_dict(orient="records")
    )
    top_out = (
        nodes.sort_values("out_weight", ascending=False)
        .head(10)[["did_hash", "out_weight"]]
        .to_dict(orient="records")
    )
    summary = {
        "n_nodes": int(len(nodes)),
        "n_edges": int(len(edges)),
        "total_weight": total_weight,
        "top_in": top_in,
        "top_out": top_out,
    }
    out_summary = Path(f"{out_prefix}_summary.json")
    out_summary.write_bytes(orjson.dumps(summary))
    print(f"Wrote reply network artifacts with prefix: {out_prefix}")


if __name__ == "__main__":
    main()
