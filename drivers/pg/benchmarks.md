# Temporal Pathfinding Benchmarks

Machine: Apple M3 Max, Postgres local, default config (`shared_buffers=128MB`, `temp_buffers=8MB`).

Query: shortest path, 6 hops, through a graph of random noise edges.

## Without temp table indexes

| Graph size              | ReadTransaction | AsOfReadTransaction | Overhead          |
| ----------------------- | --------------- | ------------------- | ----------------- |
| 10k nodes / 50k edges   | 745ms           | 405ms               | **-46% (faster)** |
| 100k nodes / 500k edges | 1,100ms         | 1,200ms             | **+10%**          |
| 1M nodes / 5M edges     | 1,500ms         | 5,500ms             | **+3.5x**         |

At small scale, asOf is faster — temp tables use session-local memory (no shared buffer locking, no WAL). At 500k edges the overhead is negligible. At 5M edges the unindexed temp table dominates.

## With temp table indexes (start_id, end_id, kind_id)

Added `CREATE INDEX` on the reconstructed temp edge table to match the real table's indexes.

| Graph size              | ReadTransaction | AsOfReadTransaction | Overhead          |
| ----------------------- | --------------- | ------------------- | ----------------- |
| 10k nodes / 50k edges   | 820ms           | 510ms               | **-38% (faster)** |
| 100k nodes / 500k edges | 1,220ms         | 5,200ms             | **+4.3x**         |

Indexes helped at small scale but made no difference at 500k. The bottleneck is not pathfinding on the temp table — it's building it. The `CREATE TEMP TABLE ... AS SELECT ... UNION ALL ...` copies the entire graph. Pathfinding indexes can't help if construction is the cost.

## With temp table indexes + deletion log index

Added `(graph_id, deleted_at, created_at)` index on `edge_deletion_log` and `node_deletion_log`.

| Graph size              | ReadTransaction | AsOfReadTransaction | Overhead          |
| ----------------------- | --------------- | ------------------- | ----------------- |
| 10k nodes / 50k edges   | 820ms           | 515ms               | **-37% (faster)** |
| 100k nodes / 500k edges | 2,300ms         | 5,250ms             | **+2.3x**         |

Deletion log index made no meaningful difference to asOf (5,200ms → 5,250ms). The deletion log is small in this benchmark (5k deletes) — the index will matter more in production with accumulated churn. Current read got slower in this run (1,220ms → 2,300ms), likely machine noise.

**Conclusion:** At 500k edges the dominant cost is the `CREATE TEMP TABLE ... AS` copying the entire graph into a temp table. Neither temp table indexes nor deletion log indexes address this. Improving reconstruction at scale requires a fundamentally different approach (e.g. materialized snapshots, incremental reconstruction).

## Setup

6-hop attack path (User → Group × 4 → Computer → DomainAdmin) embedded in random noise. Post-mark churn: 10k edges added, 5k deleted.

```
PG_CONNECTION_STRING=... go test -tags manual_integration -run='^$' -bench=BenchmarkAsOf -count=10 -v ./drivers/pg/
```
