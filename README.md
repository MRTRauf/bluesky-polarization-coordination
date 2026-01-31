bluesky-polarization-coordination
Bluesky Jetstream ingest to reply networks, polarization metrics, and coordination signals.

What this repo does
- Bluesky Jetstream ingestion to parquet
- Reply network construction
- Polarization metrics on reply network (communities/modularity/cross_ratio)
- Coordination detection via shared reply targets within time windows
- Privacy: hashed identifiers (did_hash, reply_parent_did_hash)

Pipeline overview

```mermaid
flowchart TD
  A[Jetstream] --> B[posts.parquet]
  B --> C[reply network]
  C --> D[polarization metrics]
  B --> E[coordination targets]
  E --> F[clusters/accounts]
```

Visual summary

```powershell
$clusters = Get-ChildItem artifacts\coordtargets_*_clusters.csv | Sort-Object LastWriteTime | Select-Object -Last 1
py -m src.render_report --clusters-csv $clusters.FullName --out-dir assets
```

![Coordination cluster sizes](assets/coord_cluster_sizes.png)

Top clusters table: assets/top_clusters.md

Data and privacy
- Raw Jetstream events are not committed
- Stored identifiers are hashes of DIDs and reply targets
- data/sample/posts_sample.parquet is included for quickstart

Quickstart (no live capture, uses committed sample)

```powershell
py -m src.build_reply_network --in-parquet data\sample\posts_sample.parquet
$replynet = Get-ChildItem artifacts\replynet_*_summary.json | Sort-Object LastWriteTime | Select-Object -Last 1
$edges = $replynet.FullName -replace "_summary.json","_edges.parquet"
py -m src.polarization_reply --edges-parquet $edges --posts-parquet data\sample\posts_sample.parquet
py -m src.coordination_targets --in-parquet data\sample\posts_sample.parquet --window-minutes 10 --min-target-posts 2 --max-bucket-accounts 30
```

Artifacts are generated under artifacts/ and are not committed.

Live capture (repro steps)

```powershell
py -m src.ingest_jetstream --minutes 20
$latest_parq = Get-ChildItem data\processed\posts_*.parquet | Sort-Object LastWriteTime | Select-Object -Last 1
py -m src.build_reply_network --in-parquet $latest_parq.FullName
$replynet = Get-ChildItem artifacts\replynet_*_summary.json | Sort-Object LastWriteTime | Select-Object -Last 1
$edges = $replynet.FullName -replace "_summary.json","_edges.parquet"
py -m src.polarization_reply --edges-parquet $edges --posts-parquet $latest_parq.FullName
py -m src.coordination_targets --in-parquet $latest_parq.FullName --window-minutes 10 --min-target-posts 2 --max-bucket-accounts 30
```

If a mega-cluster appears, lower max_bucket_accounts (for example 30 -> 20).

Outputs (Artifacts)

| Pattern | Contents |
| --- | --- |
| replynet_*_edges.parquet | Reply edges (src, dst, weight) |
| replynet_*_summary.json | Reply network summary stats |
| polarization_*_metrics.json | Modularity, communities, cross-community ratio |
| coordtargets_*_clusters.csv | Connected components with top target |
| coordtargets_*_accounts.csv | Account cluster membership and coord_score |
| coordtargets_*_summary.json | Coordination summary stats |

Results snapshot
Sample run (sample parquet) is a smoke test and will vary by sample selection.
Full run example is captured in REPORT.md and does not auto-update.

Troubleshooting
- Git dubious ownership: `git config --global --add safe.directory D:\!Project\bluesky-polarization-coordination`
- PowerShell quoting: prefer `-LiteralPath` for paths with special characters
- Coordination yields none: lower min_target_posts to 2 for short captures, raise it for longer captures

Project structure

```
bluesky-polarization-coordination/
  artifacts/ (generated)
  data/
    raw/ (generated)
    processed/ (generated)
    sample/
  src/
```
