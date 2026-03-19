# Temporal Pathfinding Benchmarks

Machine: Apple M3 Max, Postgres local, default config (`shared_buffers=128MB`, `temp_buffers=8MB`).

Query: shortest path, 6 hops, through a graph of random noise edges.

## Baseline (default Postgres config)

| Graph size              | ReadTransaction | AsOfReadTransaction | Overhead          |
| ----------------------- | --------------- | ------------------- | ----------------- |
| 10k nodes / 50k edges   | 745ms           | 405ms               | **-46% (faster)** |
| 100k nodes / 500k edges | 1,100ms         | 1,200ms             | **+10%**          |
| 1M nodes / 5M edges     | 1,500ms         | 5,500ms             | **+3.5x**         |

At small scale, asOf is faster — temp tables use session-local memory (no shared buffer locking, no WAL). At 5M edges, reconstruction dominates.

## Experiment: temp table indexes (start_id, end_id, kind_id)

Added `CREATE INDEX` on the reconstructed temp edge table to match the real table's indexes.

| Graph size              | ReadTransaction | AsOfReadTransaction | Overhead          |
| ----------------------- | --------------- | ------------------- | ----------------- |
| 10k nodes / 50k edges   | 820ms           | 510ms               | **-38% (faster)** |
| 100k nodes / 500k edges | 1,220ms         | 5,200ms             | **+4.3x**         |

No improvement at 500k. The bottleneck is building the temp table, not querying it. **Reverted.**

## Experiment: deletion log index (graph_id, deleted_at, created_at)

Added index on `edge_deletion_log` and `node_deletion_log` to speed up the reconstruction UNION ALL.

| Graph size              | ReadTransaction | AsOfReadTransaction | Overhead          |
| ----------------------- | --------------- | ------------------- | ----------------- |
| 10k nodes / 50k edges   | 820ms           | 515ms               | **-37% (faster)** |
| 100k nodes / 500k edges | 2,300ms         | 5,250ms             | **+2.3x**         |

No meaningful change to asOf (5,200ms → 5,250ms). The deletion log is small in this benchmark (5k deletes). Would matter more in production with accumulated churn. **Reverted.**

## Experiment: temp_buffers = 256MB

Increased `temp_buffers` from 8MB (default) to 256MB so the reconstructed temp table stays in memory instead of spilling to disk.

| Graph size              | ReadTransaction | AsOfReadTransaction | Overhead          |
| ----------------------- | --------------- | ------------------- | ----------------- |
| 10k nodes / 50k edges   | 840ms           | 448ms               | **-47% (faster)** |
| 100k nodes / 500k edges | 1,250ms         | 1,090ms             | **-13% (faster)** |
| 1M nodes / 5M edges     | 1,830ms         | 6,000ms             | **+3.3x**         |

At 500k edges this turned a 4x slowdown into a 13% speedup. The entire bottleneck at this scale was disk spill — the temp table (~75-100MB) exceeded the default 8MB `temp_buffers` and Postgres wrote it to disk and read it back. With enough memory, reconstruction is cheap and the temp table is fast to query.

At 5M edges the result is unchanged from baseline (3.5x → 3.3x) — 256MB isn't enough for a ~750MB temp table. Confirms it's the same disk spill behavior. You'd need ~1GB of `temp_buffers` to keep 5M edges in memory, and that's per-session.

This is a physical limit. At scales where the temp table exceeds available memory, reconstruction will always pay for disk I/O. The only levers are giving Postgres more memory or reducing what gets copied (materialized snapshots, incremental reconstruction).

## Setup

6-hop attack path (User → Group × 4 → Computer → DomainAdmin) embedded in random noise. Post-mark churn: 10k edges added, 5k deleted.

```
PG_CONNECTION_STRING=... go test -tags manual_integration -run='^$' -bench=BenchmarkAsOf -count=10 -v ./drivers/pg/
```
