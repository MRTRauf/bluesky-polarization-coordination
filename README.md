bluesky-polarization-coordination

Bluesky Jetstream ingest -> reply network -> polarization metrics -> coordination via shared reply targets.
Privacy: author DIDs and reply targets are hashed (sha256 hex) before storage.

What this repo demonstrates
- Jetstream ingestion to structured Parquet for network analysis
- Reply network construction with weighted edges
- Louvain community detection and polarization metrics
- Coordination detection via co-reply targets in time windows
- Lightweight artifacts for reproducible inspection

Setup (Windows PowerShell)

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
```

Quickstart (no live capture)

```powershell
py -m src.make_sample --in-parquet data\processed\posts_YYYYMMDDTHHMMSSZ.parquet --out-parquet data\sample\posts_sample.parquet --n 2000
py -m src.build_reply_network --in-parquet data\sample\posts_sample.parquet
py -m src.polarization_reply --edges-parquet artifacts\replynet_YYYYMMDDTHHMMSSZ_edges.parquet --posts-parquet data\sample\posts_sample.parquet
py -m src.coordination_targets --in-parquet data\sample\posts_sample.parquet --window-minutes 10 --min-target-posts 2 --max-bucket-accounts 30
```

Live capture

```powershell
py -m src.ingest_jetstream --minutes 5
py -m src.build_reply_network --in-parquet data\processed\posts_YYYYMMDDTHHMMSSZ.parquet
py -m src.polarization_reply --edges-parquet artifacts\replynet_YYYYMMDDTHHMMSSZ_edges.parquet --posts-parquet data\processed\posts_YYYYMMDDTHHMMSSZ.parquet
py -m src.coordination_targets --in-parquet data\processed\posts_YYYYMMDDTHHMMSSZ.parquet --window-minutes 10 --min-target-posts 2 --max-bucket-accounts 30
```

Artifacts

| Path | Contents |
| --- | --- |
| data/raw/jetstream_<UTC>.jsonl | Raw Jetstream events, one JSON per line |
| data/processed/posts_<UTC>.parquet | Post records with hashed identifiers |
| artifacts/replynet_<UTC>_edges.parquet | Reply edges (src, dst, weight) |
| artifacts/replynet_<UTC>_nodes.parquet | Reply node weights |
| artifacts/replynet_<UTC>_summary.json | Reply network summary stats |
| artifacts/polarization_<UTC>_metrics.json | Modularity, communities, cross-community ratio |
| artifacts/polarization_<UTC>_communities.parquet | did_hash to community_id |
| artifacts/polarization_<UTC>_community_sizes.csv | Community sizes |
| artifacts/polarization_<UTC>_community_terms.csv | Top TF-IDF terms per community |
| artifacts/coordtargets_<UTC>_edges.parquet | Co-reply edges |
| artifacts/coordtargets_<UTC>_targets.csv | Reply targets and buckets used |
| artifacts/coordtargets_<UTC>_clusters.csv | Connected components with top target |
| artifacts/coordtargets_<UTC>_accounts.csv | Account cluster membership and coord_score |
| artifacts/coordtargets_<UTC>_summary.json | Coordination summary stats |

Results (example run, 2026-01-31)
- posts=49333 replies=20843 from data/processed/posts_20260131T123349Z.parquet
- polarization modularity=0.9857 cross_ratio=0.0088 from artifacts/polarization_20260131T125509Z_metrics.json
- coordination clusters=303 edges=5077 from artifacts/coordtargets_20260131T130156Z_summary.json
- If your artifacts differ, copy metrics from the matching *_summary.json or *_metrics.json files.

Project structure

```
bluesky-polarization-coordination/
  artifacts/
  data/
    raw/
    processed/
    sample/
  src/
    build_reply_network.py
    coordination_links.py
    coordination_targets.py
    ingest_jetstream.py
    inspect_raw_jsonl.py
    make_sample.py
    polarization_reply.py
    paths.py
```
