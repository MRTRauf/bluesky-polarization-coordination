bluesky-polarization-coordination

Bluesky Jetstream ingest -> reply network -> polarization metrics -> coordination via shared reply targets.
Privacy: author DIDs and reply targets are hashed (sha256 hex) before storage.

What this repo demonstrates
- Bluesky Jetstream ingestion
- Reply network construction
- Polarization metrics (modularity, cross-community ratio)
- Coordination detection via shared reply targets
- Privacy-preserving hashing

Setup (Windows PowerShell)

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
```

Quickstart (no live capture)

```powershell
py -m src.build_reply_network --in-parquet data\sample\posts_sample.parquet
$replynet = Get-ChildItem artifacts\replynet_*_summary.json | Sort-Object LastWriteTime | Select-Object -Last 1
$edges = $replynet.FullName -replace "_summary.json","_edges.parquet"
py -m src.polarization_reply --edges-parquet $edges --posts-parquet data\sample\posts_sample.parquet
py -m src.coordination_targets --in-parquet data\sample\posts_sample.parquet --window-minutes 10 --min-target-posts 2 --max-bucket-accounts 30
```

Live capture

```powershell
py -m src.ingest_jetstream --minutes 20
$latest_parq = Get-ChildItem data\processed\posts_*.parquet | Sort-Object LastWriteTime | Select-Object -Last 1
py -m src.build_reply_network --in-parquet $latest_parq.FullName
$replynet = Get-ChildItem artifacts\replynet_*_summary.json | Sort-Object LastWriteTime | Select-Object -Last 1
$edges = $replynet.FullName -replace "_summary.json","_edges.parquet"
py -m src.polarization_reply --edges-parquet $edges --posts-parquet $latest_parq.FullName
py -m src.coordination_targets --in-parquet $latest_parq.FullName --window-minutes 10 --min-target-posts 2 --max-bucket-accounts 30
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

Troubleshooting
- Git safe.directory: `git config --global --add safe.directory D:\!Project\bluesky-polarization-coordination`
- If a coordination cluster is too large, lower `--max-bucket-accounts` to suppress broad buckets

Results (example run, 2026-01-31)
- posts=49333 replies=20843 from data/processed/posts_20260131T123349Z.parquet
- polarization modularity=0.9857 cross_ratio=0.0088 from artifacts/polarization_20260131T125509Z_metrics.json
- coordination clusters=303 edges=5077 from artifacts/coordtargets_20260131T130156Z_summary.json
- If your artifacts differ, copy metrics from the matching *_summary.json or *_metrics.json files.
