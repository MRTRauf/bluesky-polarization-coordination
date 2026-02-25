"""
Topic modeling with BERTopic for Bluesky posts.
"""

import argparse
import json
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from .paths import ARTIFACTS_DIR, ensure_dirs

URL_RE = re.compile(r"https?://\S+|www\.\S+")
MENTION_RE = re.compile(r"@\w+")
HASHTAG_RE = re.compile(r"#\w+")
EMOJI_RE = re.compile(
    "[\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\U00002700-\U000027BF"
    "\U00002600-\U000026FF]+",
    flags=re.UNICODE,
)
WS_RE = re.compile(r"\s+")
ALPHA_TOKEN_RE = re.compile(r"(?u)\b[^\W\d_][^\W\d_]+\b")

NOISE_STOPWORDS = {
    "bsky",
    "bskyapp",
    "reply",
    "repost",
    "like",
    "follow",
    "https",
    "http",
    "www",
    "com",
    "org",
    "net",
    "amp",
}
COMMON_MULTI_STOPWORDS = {
    "dan",
    "yang",
    "untuk",
    "dengan",
    "ini",
    "itu",
    "dari",
    "atau",
    "nicht",
    "und",
    "der",
    "die",
    "das",
    "mit",
    "para",
    "pero",
    "que",
    "con",
    "por",
    "une",
    "des",
    "les",
    "pour",
    "avec",
    "est",
    "это",
    "как",
    "для",
    "что",
    "the",
    "and",
    "for",
    "with",
}


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_ngram_range(value: str) -> tuple[int, int]:
    parts = [p.strip() for p in str(value).split(",")]
    if len(parts) != 2:
        raise ValueError("ngram-range must be two integers, e.g. 1,2")
    lo, hi = int(parts[0]), int(parts[1])
    if lo < 1 or hi < lo:
        raise ValueError("ngram-range values are invalid")
    return lo, hi


def parse_stopwords_extra(value: Optional[str]) -> set[str]:
    if not value:
        return set()
    path = Path(value)
    tokens = []
    if path.exists() and path.is_file():
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            tokens.extend([x.strip().lower() for x in line.split(",") if x.strip()])
    else:
        tokens = [x.strip().lower() for x in value.split(",") if x.strip()]
    return set(tokens)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run BERTopic on Bluesky posts parquet.")
    parser.add_argument("--posts-parquet", required=True, help="Input posts parquet.")
    parser.add_argument("--out-prefix", default=None, help="Output prefix for artifacts.")
    parser.add_argument("--sample-n", type=int, default=20000, help="Maximum posts used for modeling.")
    parser.add_argument("--min-topic-size", type=int, default=50, help="Minimum BERTopic topic size.")
    parser.add_argument("--min-df", type=int, default=5, help="Minimum document frequency for vectorizer.")
    parser.add_argument("--max-df", type=float, default=0.6, help="Maximum document frequency ratio.")
    parser.add_argument("--ngram-range", default="1,2", help="Ngram range as min,max.")
    parser.add_argument(
        "--stopwords-extra",
        default=None,
        help="Extra stopwords as comma-separated list or path to text file.",
    )
    return parser.parse_args()


def clean_text(text: str) -> str:
    value = str(text).lower()
    value = URL_RE.sub(" ", value)
    value = MENTION_RE.sub(" ", value)
    value = HASHTAG_RE.sub(" ", value)
    value = EMOJI_RE.sub(" ", value)
    value = WS_RE.sub(" ", value).strip()
    return value


def load_posts(posts_path: Path) -> pd.DataFrame:
    try:
        import pyarrow.parquet as pq
    except Exception:
        cols = ["did_hash", "text", "created_at", "time_us"]
        posts = pd.read_parquet(posts_path)
        available = [c for c in cols if c in posts.columns]
        posts = posts[available].copy()
        return normalize_created_at(posts)
    parquet = pq.ParquetFile(posts_path)
    schema_cols = set(parquet.schema.names)
    base_cols = [c for c in ["did_hash", "text"] if c in schema_cols]
    extra_cols = [c for c in ["created_at", "time_us"] if c in schema_cols]
    if "did_hash" not in base_cols or "text" not in base_cols:
        raise ValueError("Input parquet must contain did_hash and text columns.")
    posts = pd.read_parquet(posts_path, columns=base_cols + extra_cols)
    return normalize_created_at(posts)


def normalize_created_at(posts: pd.DataFrame) -> pd.DataFrame:
    if "created_at" not in posts.columns:
        if "time_us" in posts.columns:
            created = pd.to_datetime(posts["time_us"], unit="us", utc=True, errors="coerce")
            posts["created_at"] = created.astype(str)
        else:
            posts["created_at"] = ""
    posts["did_hash"] = posts["did_hash"].astype(str)
    posts["text"] = posts["text"].fillna("").astype(str)
    posts["created_at"] = posts["created_at"].fillna("").astype(str)
    return posts[["did_hash", "created_at", "text"]].copy()


def combined_stopwords(extra_stopwords: set[str]) -> set[str]:
    try:
        from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
        english = set(ENGLISH_STOP_WORDS)
    except Exception:
        english = {
            "the",
            "a",
            "an",
            "to",
            "of",
            "for",
            "in",
            "on",
            "and",
            "is",
            "are",
            "was",
            "were",
            "be",
            "it",
        }
    return english | NOISE_STOPWORDS | COMMON_MULTI_STOPWORDS | extra_stopwords


def build_model(
    min_topic_size: int,
    min_df: int,
    max_df: float,
    ngram_range: tuple[int, int],
    stopwords: set[str],
):
    try:
        from bertopic import BERTopic
        from hdbscan import HDBSCAN
        from sentence_transformers import SentenceTransformer
        from sklearn.feature_extraction.text import CountVectorizer
        from umap import UMAP
    except Exception as exc:
        print("BERTopic dependencies are missing.")
        print("Install with: py -m pip install bertopic sentence-transformers umap-learn hdbscan")
        print(f"Import error: {exc}")
        return None, None, None
    embedding_name = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    try:
        embedding_model = SentenceTransformer(embedding_name)
    except Exception as exc:
        print("Could not load sentence-transformers model.")
        print("Check internet or local model cache, then retry.")
        print(f"Model error: {exc}")
        return None, None, None
    vectorizer_model = CountVectorizer(
        ngram_range=ngram_range,
        min_df=int(min_df),
        max_df=float(max_df),
        stop_words=sorted(stopwords),   # <-- FIX
        token_pattern=r"(?u)\b[^\W\d_][^\W\d_]+\b",
    )
    umap_model = UMAP(
        n_neighbors=15,
        n_components=5,
        min_dist=0.0,
        metric="cosine",
        random_state=42,
    )
    hdbscan_model = HDBSCAN(
        min_cluster_size=max(2, int(min_topic_size)),
        metric="euclidean",
        cluster_selection_method="eom",
        prediction_data=True,
    )
    model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        min_topic_size=int(min_topic_size),
        calculate_probabilities=True,
        verbose=False,
    )
    params = {
        "embedding_model": embedding_name,
        "min_topic_size": int(min_topic_size),
        "min_df": int(min_df),
        "max_df": float(max_df),
        "ngram_range": [int(ngram_range[0]), int(ngram_range[1])],
        "umap_random_state": 42,
    }
    return model, embedding_name, params


def assigned_prob(topics: list[int], probs) -> list[Optional[float]]:
    if probs is None:
        return [None] * len(topics)
    if not hasattr(probs, "shape"):
        return [None] * len(topics)
    if len(probs.shape) == 1:
        return [float(probs[i]) for i in range(len(topics))]
    out = []
    for i, topic_id in enumerate(topics):
        if topic_id is None or int(topic_id) < 0:
            out.append(None)
            continue
        if int(topic_id) >= probs.shape[1]:
            out.append(None)
            continue
        out.append(float(probs[i, int(topic_id)]))
    return out


def representative_posts(texts: pd.Series, probs: pd.Series) -> list[str]:
    frame = pd.DataFrame({"text": texts.astype(str), "prob": probs})
    frame = frame.sort_values("prob", ascending=False, na_position="last")
    out = []
    for value in frame["text"].head(3).tolist():
        short = WS_RE.sub(" ", value).strip()
        if len(short) > 180:
            short = short[:177] + "..."
        out.append(short)
    while len(out) < 3:
        out.append("")
    return out[:3]


def topic_is_noise(topic_words: list[str], stopwords: set[str]) -> bool:
    words = [w.strip().lower() for w in topic_words if isinstance(w, str) and w.strip()]
    if not words:
        return True
    informative = 0
    for phrase in words:
        parts = [p for p in phrase.split() if p]
        if not parts:
            continue
        if any(ALPHA_TOKEN_RE.fullmatch(part) and part not in stopwords for part in parts):
            informative += 1
    return informative < max(2, int(0.4 * len(words)))


def build_topics_table(model, doc_frame: pd.DataFrame, stopwords: set[str]) -> pd.DataFrame:
    topic_info = model.get_topic_info()
    rows = []
    for row in topic_info.itertuples(index=False):
        topic_id = int(row.Topic)
        if topic_id < 0:
            continue
        words = model.get_topic(topic_id) or []
        top_word_list = [str(w) for w, _ in words[:10]]
        if topic_is_noise(top_word_list, stopwords):
            continue
        top_words = ", ".join(top_word_list)
        topic_docs = doc_frame[doc_frame["topic_id"] == topic_id]
        reps = representative_posts(topic_docs["clean_text"], topic_docs["topic_prob"])
        rows.append(
            {
                "topic_id": topic_id,
                "topic_size": int(row.Count),
                "top_words": top_words,
                "representative_posts": " | ".join(reps),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["topic_id", "topic_size", "top_words", "representative_posts"])
    return pd.DataFrame(rows).sort_values("topic_size", ascending=False).reset_index(drop=True)


def main() -> None:
    args = parse_args()
    ensure_dirs()
    random.seed(42)
    start = time.time()
    posts_path = Path(args.posts_parquet)
    out_prefix = Path(args.out_prefix) if args.out_prefix else ARTIFACTS_DIR / f"topicmodel_{utc_stamp()}"
    ngram_range = parse_ngram_range(args.ngram_range)
    extra_stopwords = parse_stopwords_extra(args.stopwords_extra)
    stopwords = combined_stopwords(extra_stopwords)
    posts = load_posts(posts_path)
    if posts.empty:
        print("No posts available for topic modeling.")
        return
    posts["clean_text"] = posts["text"].map(clean_text)
    posts = posts[posts["clean_text"].str.len() > 0].copy()
    if posts.empty:
        print("No valid text after cleaning.")
        return
    if args.sample_n and len(posts) > int(args.sample_n):
        posts = posts.sample(n=int(args.sample_n), random_state=42).reset_index(drop=True)
    model, model_name, model_params = build_model(
        args.min_topic_size,
        args.min_df,
        args.max_df,
        ngram_range,
        stopwords,
    )
    if model is None:
        return
    texts = posts["clean_text"].tolist()
    topics, probs = model.fit_transform(texts)
    posts["topic_id"] = [int(x) for x in topics]
    posts["topic_prob"] = assigned_prob(posts["topic_id"].tolist(), probs)
    topic_table = build_topics_table(model, posts, stopwords)
    kept_topics = set(topic_table["topic_id"].astype(int).tolist())
    doc_topics = posts[["did_hash", "created_at", "topic_id", "topic_prob"]].copy()
    if kept_topics:
        doc_topics.loc[~doc_topics["topic_id"].isin(kept_topics), "topic_id"] = -1
        doc_topics.loc[doc_topics["topic_id"] < 0, "topic_prob"] = None
    else:
        doc_topics["topic_id"] = -1
        doc_topics["topic_prob"] = None
    topics_path = Path(f"{out_prefix}_topics.csv")
    docs_path = Path(f"{out_prefix}_doc_topics.parquet")
    summary_path = Path(f"{out_prefix}_topic_summary.json")
    topic_table.to_csv(topics_path, index=False)
    doc_topics.to_parquet(docs_path, index=False)
    example_words = topic_table["top_words"].head(3).tolist() if not topic_table.empty else []
    summary = {
        "n_posts_used": int(len(posts)),
        "n_topics": int(topic_table["topic_id"].nunique()) if not topic_table.empty else 0,
        "runtime_seconds": round(time.time() - start, 3),
        "example_top_words": example_words,
        "model_params": {
            "bertopic_model": "BERTopic",
            "embedding_model": model_name,
            "sample_n": int(args.sample_n),
            "stopwords_extra_count": int(len(extra_stopwords)),
            **model_params,
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Saved topics: {topics_path}")
    print(f"Saved doc topics: {docs_path}")
    print(f"Saved summary: {summary_path}")
    top10 = topic_table.head(10)
    if top10.empty:
        print("No high-quality topics after filtering.")
        return
    print("Top topics by size:")
    for row in top10.itertuples(index=False):
        print(f"topic={row.topic_id} size={row.topic_size} words={row.top_words}")
        print(f"examples={row.representative_posts}")


if __name__ == "__main__":
    main()
