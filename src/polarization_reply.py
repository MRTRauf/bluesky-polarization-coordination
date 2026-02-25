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
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS
import re

from .paths import ARTIFACTS_DIR, ensure_dirs


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute polarization metrics and community keywords from reply network."
    )

    # Required inputs
    parser.add_argument(
        "--edges-parquet",
        required=True,
        help="Input reply edges parquet."
    )
    parser.add_argument(
        "--posts-parquet",
        required=True,
        help="Input posts parquet."
    )

    # Output control
    parser.add_argument(
        "--out-prefix",
        default=None,
        help="Output prefix for artifacts (default: artifacts/polarization_<UTCSTAMP>)."
    )

    # Keyword extraction controls
    parser.add_argument(
        "--topk-terms",
        type=int,
        default=25,
        help="Number of top TF-IDF terms per community (default: 25)."
    )
    parser.add_argument(
        "--max-posts-per-community",
        type=int,
        default=2000,
        help="Maximum number of posts per community used for TF-IDF (default: 2000)."
    )
    parser.add_argument(
        "--min-community-posts",
        type=int,
        default=50,
        help="Minimum number of posts required for a community to be included in TF-IDF (default: 50)."
    )
    parser.add_argument(
        "--min-df",
        type=int,
        default=2,
        help="Minimum document frequency across community-docs (default: 2)."
    )
    parser.add_argument(
        "--max-df",
        type=float,
        default=0.7,
        help="Maximum document frequency ratio across community-docs (default: 0.7)."
    )

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

URL_RE = re.compile(r"https?://\S+|www\.\S+")
MENTION_RE = re.compile(r"@\w+")
HANDLE_RE = re.compile(r"\b(bsky\.app|youtu\.be|youtube|t\.co)\b", re.IGNORECASE)
WS_RE = re.compile(r"\s+")

# Stopwords tambahan (ringan) untuk membuat keywords lebih "tematik"
GERMAN_STOP = {
    "und","oder","aber","dass","das","die","der","den","dem","ein","eine","einen","einem","einer",
    "ist","sind","war","waren","zu","zum","zur","im","in","am","an","auf","mit","von","für","nicht",
    "ich","du","er","sie","es","wir","ihr","mein","dein","sein","ihr","euch","uns"
}
INDO_STOP = {
    "dan","atau","tapi","yang","ini","itu","di","ke","dari","untuk","pada","dengan","tidak","iya",
    "saya","aku","kamu","dia","mereka","kami","kita","anda","nya","lah","pun","jadi","karena"
}

NOISE_TOKENS = {
    "https","http","www","com","org","net","jpg","png","gif","amp","rt",
    "reply","repost","like","follow"
}

def make_stopwords() -> set[str]:
    # gabungkan stopwords English + German + Indonesian + noise token
    return set(ENGLISH_STOP_WORDS) | GERMAN_STOP | INDO_STOP | NOISE_TOKENS

def clean_text(s: str) -> str:
    s = s or ""
    s = s.lower()
    s = URL_RE.sub(" ", s)
    s = MENTION_RE.sub(" ", s)
    s = HANDLE_RE.sub(" ", s)
    s = s.replace("\u200b", " ")  # zero-width space
    s = WS_RE.sub(" ", s).strip()
    return s

def compute_community_terms(
    posts: pd.DataFrame,
    communities: pd.DataFrame,
    out_path: Path,
    *,
    topk_terms: int = 25,
    max_posts_per_community: int = 2000,
    min_community_posts: int = 50,
    min_df: int = 2,
    max_df: float = 0.7,
) -> None:
    merged = posts.merge(communities, on="did_hash", how="inner")
    if merged.empty:
        out_path.write_text("community_id,term,score\n", encoding="utf-8")
        return

    # basic cleaning
    merged["text"] = merged["text"].fillna("").astype(str).map(clean_text)

    # drop empty text rows early
    merged = merged[merged["text"].str.len() > 0]
    if merged.empty:
        out_path.write_text("community_id,term,score\n", encoding="utf-8")
        return

    # skip tiny communities to avoid "stop 1.0" style artifacts
    comm_counts = merged.groupby("community_id").size()
    keep_ids = comm_counts[comm_counts >= min_community_posts].index
    merged = merged[merged["community_id"].isin(keep_ids)]
    if merged.empty:
        out_path.write_text("community_id,term,score\n", encoding="utf-8")
        return

    # limit posts per community
    limited = merged.groupby("community_id", sort=False).head(max_posts_per_community)

    # build one "document" per community
    grouped = (
        limited.groupby("community_id")["text"]
        .apply(lambda x: " ".join(x))
        .reset_index()
    )
    docs = grouped["text"].tolist()
    if not docs:
        out_path.write_text("community_id,term,score\n", encoding="utf-8")
        return

    stopwords = make_stopwords()

    # Unicode-friendly token pattern: letters only (no digits/underscore)
    token_pattern = r"(?u)\b[^\W\d_][^\W\d_]+\b"

    vectorizer = TfidfVectorizer(
        min_df=min_df,
        max_df=max_df,
        max_features=30000,
        stop_words=stopwords,
        ngram_range=(1, 2),
        token_pattern=token_pattern,
        lowercase=True,
        sublinear_tf=True,
        strip_accents="unicode",
    )

    tfidf = vectorizer.fit_transform(docs)
    terms = vectorizer.get_feature_names_out()

    rows = []
    for idx, community_id in enumerate(grouped["community_id"].tolist()):
        row = tfidf.getrow(idx)
        if row.nnz == 0:
            continue
        scores = row.toarray().ravel()
        top_indices = scores.argsort()[-topk_terms:][::-1]
        for term_idx in top_indices:
            score = float(scores[term_idx])
            if score <= 0:
                continue
            rows.append({"community_id": int(community_id), "term": terms[term_idx], "score": score})

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
    compute_community_terms(
    posts,
    communities,
    Path(f"{out_prefix}_community_terms.csv"),
    topk_terms=args.topk_terms,
    max_posts_per_community=args.max_posts_per_community,
    min_community_posts=args.min_community_posts,
    min_df=args.min_df,
    max_df=args.max_df,
    )


if __name__ == "__main__":
    main()
