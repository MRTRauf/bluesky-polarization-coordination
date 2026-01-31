"""
Render a simple visual summary from coordination clusters.
"""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render coordination report assets.")
    parser.add_argument("--clusters-csv", required=True, help="Input clusters CSV.")
    parser.add_argument("--out-dir", default="assets", help="Output directory.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    in_path = Path(args.clusters_csv)
    if not in_path.exists():
        raise FileNotFoundError(f"Clusters CSV not found: {in_path}")
    df = pd.read_csv(in_path)
    required = {"cluster_id", "n_accounts", "total_edge_weight", "top_target_share"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    sizes = df["n_accounts"].astype(int)
    plt.figure()
    plt.hist(sizes, bins=30)
    plt.title("Coordination cluster sizes (n_accounts)")
    plt.xlabel("Accounts per cluster")
    plt.ylabel("Number of clusters")
    fig_path = out_dir / "coord_cluster_sizes.png"
    plt.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close()
    top = df.sort_values("n_accounts", ascending=False).head(10)
    md_path = out_dir / "top_clusters.md"
    lines = ["cluster_id,n_accounts,total_edge_weight,top_target_share"]
    for row in top.itertuples(index=False):
        lines.append(
            f"{row.cluster_id},{row.n_accounts},{row.total_edge_weight},{row.top_target_share}"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {fig_path} and {md_path}")


if __name__ == "__main__":
    main()
