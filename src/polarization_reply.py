"""
Polarization metrics and community terms from reply network.
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import networkx as nx
import orjson
import pandas as pd
from community import community_louvain
from sklearn.feature_extraction.text import TfidfVectorizer

from .paths import ARTIFACTS_DIR, ensure_dirs


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute polarization metrics from reply network.")
    parser.add_argument("--edges-parquet", required=True, help="Input reply edges parquet.")
    parser.add_argument("--posts-parquet", required=True, help="Input posts parquet.")
    parser.add_argument("--out-prefix", default=None, help="Output prefix for artifacts.")
    return parser.parse_args()


def build_undirected_graph(edges: pd.DataFrame) -> nx.Graph:
    graph = nx.Graph()
    for _, row in edges.iterrows():
        src = row["src"]
        dst = row["dst"]
        weight = int(row["weight"])
        if graph.has_edge(src, dst):
            graph[src][dst]["weight"] += weight
        else:
            graph.add_edge(src, dst, weight=weight)
    return graph


def compute_community_terms(
    posts: pd.DataFrame,
    communities: pd.DataFrame,
    out_path: Path,
) -> None:
    merged = posts.merge(communities, on="did_hash", how="inner")
    if merged.empty:
        out_path.write_text("community_id,term,score\n", encoding="utf-8")
        return
    merged["text"] = merged["text"].fillna("")
    limited = merged.groupby("community_id", sort=False).head(2000)
    grouped = limited.groupby("community_id")["text"].apply(lambda x: " ".join(x)).reset_index()
    docs = grouped["text"].tolist()
    if not docs:
        out_path.write_text("community_id,term,score\n", encoding="utf-8")
        return
    vectorizer = TfidfVectorizer(min_df=5, max_features=30000, stop_words=None)
    tfidf = vectorizer.fit_transform(docs)
    terms = vectorizer.get_feature_names_out()
    rows = []
    for idx, community_id in enumerate(grouped["community_id"].tolist()):
        row = tfidf.getrow(idx)
        if row.nnz == 0:
            continue
        scores = row.toarray().ravel()
        top_indices = scores.argsort()[-25:][::-1]
        for term_idx in top_indices:
            score = float(scores[term_idx])
            if score <= 0:
                continue
            rows.append({"community_id": community_id, "term": terms[term_idx], "score": score})
    if rows:
        pd.DataFrame(rows).to_csv(out_path, index=False)
    else:
        out_path.write_text("community_id,term,score\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    ensure_dirs()
    edges_path = Path(args.edges_parquet)
    posts_path = Path(args.posts_parquet)
    if args.out_prefix:
        out_prefix = Path(args.out_prefix)
    else:
        out_prefix = ARTIFACTS_DIR / f"polarization_{utc_stamp()}"
    edges = pd.read_parquet(edges_path)
    if edges.empty:
        print("No edges available for polarization metrics.")
        return
    graph = build_undirected_graph(edges)
    if graph.number_of_nodes() == 0 or graph.number_of_edges() == 0:
        print("Graph has no usable structure for polarization metrics.")
        return
    n_nodes = int(graph.number_of_nodes())
    n_edges = int(graph.number_of_edges())
    partition = community_louvain.best_partition(graph, weight="weight")
    modularity = community_louvain.modularity(partition, graph, weight="weight")
    communities = pd.DataFrame(
        [{"did_hash": did_hash, "community_id": int(comm)} for did_hash, comm in partition.items()]
    )
    out_communities = Path(f"{out_prefix}_communities.parquet")
    communities.to_parquet(out_communities, index=False)
    community_sizes = communities.groupby("community_id").size().reset_index(name="size")
    out_sizes = Path(f"{out_prefix}_community_sizes.csv")
    community_sizes.to_csv(out_sizes, index=False)
    total_weight = 0
    cross_weight = 0
    for u, v, data in graph.edges(data=True):
        w = float(data.get("weight", 0))
        total_weight += w
        if partition.get(u) != partition.get(v):
            cross_weight += w
    cross_ratio = (cross_weight / total_weight) if total_weight > 0 else 0.0
    largest_share = community_sizes["size"].max() / communities.shape[0]
    metrics = {
        "modularity": float(modularity),
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "n_communities": int(community_sizes.shape[0]),
        "cross_community_edge_ratio": float(cross_ratio),
        "largest_community_share": float(largest_share),
    }
    out_metrics = Path(f"{out_prefix}_metrics.json")
    out_metrics.write_bytes(orjson.dumps(metrics))
    posts = pd.read_parquet(posts_path)
    compute_community_terms(posts, communities, Path(f"{out_prefix}_community_terms.csv"))
    print(
        "polarization_ready "
        f"metrics={out_metrics} "
        f"communities={metrics['n_communities']} "
        f"modularity={modularity:.4f} "
        f"cross_ratio={cross_ratio:.4f}"
    )


if __name__ == "__main__":
    main()
