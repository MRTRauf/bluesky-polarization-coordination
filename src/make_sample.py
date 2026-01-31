"""
Create a small sample parquet file.
"""

import argparse
from pathlib import Path

import pandas as pd

from .paths import SAMPLE_DIR, ensure_dirs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a sample parquet file.")
    parser.add_argument("--in-parquet", required=True, help="Input parquet file path.")
    parser.add_argument(
        "--out-parquet",
        default=str(SAMPLE_DIR / "posts_sample.parquet"),
        help="Output parquet path.",
    )
    parser.add_argument("--n", type=int, default=2000, help="Sample size.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()
    in_path = Path(args.in_parquet)
    if not in_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {in_path}")
    df = pd.read_parquet(in_path)
    sample_n = int(args.n)
    if sample_n <= 0:
        raise ValueError("Sample size must be positive.")
    if len(df) <= sample_n:
        sample_df = df
    else:
        sample_df = df.sample(n=sample_n, random_state=7)
    out_path = Path(args.out_parquet)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sample_df.to_parquet(out_path, index=False)
    print(f"Wrote sample parquet: {out_path}")


if __name__ == "__main__":
    main()
