# Temporal Graph: Known Caveats

## Deletion logs are unindexed and unbounded

The `node_deletion_log` and `edge_deletion_log` tables have no indexes. Reconstruction
queries filter on `graph_id`, `created_at`, and `deleted_at` — all full scans today.
There is also no retention policy; logs grow indefinitely with churn.

Needs: indexes on `(graph_id, deleted_at, created_at)`, and a pruning mechanism
(e.g. `DELETE FROM edge_deletion_log WHERE deleted_at < now() - interval '90 days'`).

## Property and kind changes are invisible

Only creates and deletes are tracked. If a node's `kind_ids` or `properties` are updated,
the previous values are lost. A historical query shows nodes/edges that _existed_ at time T,
but with their _current_ properties — not the properties they had at time T.

Fixing this properly requires either an update log table or full row versioning (SCD Type 2),
both of which significantly increase write-path complexity and storage.

## Temp table restrictions (as built)

### Full reconstruction each time

`ON COMMIT DROP` means every `AsOfReadTransaction` call rebuilds the full temp tables.
Five queries against the same historical point = five reconstructions.

### Lack of indexes on temporary table

Additionally reconstructed temp tables have no indexes. The real `edge` table has btree
indexes on `start_id`, `end_id`, and `kind_id`. Pathfinding relies on these. Temporal
pathfinding queries pay for sequential scans as written Adding indexes to temp tables is
cheap at small scale but may dominate reconstruction time at large scale. TODO: Benchmark

### Non-unique temp table names

The pathfinding SQL functions (`create_unidirectional_pathspace_tables`, etc.) create temp
tables (`forward_front`, `next_front`) with `ON COMMIT DROP`. Since `AsOfReadTransaction`
runs everything in a single transaction (to keep the temporal temp tables alive), running
multiple pathfinding queries in one delegate causes `relation already exists` errors.

### Produciton solutions

- Cache the last `n` analysis runs. TODO: Assess for size feasibility
- Allow a point-in-time "snapshot" set by the user, persisted $time

## No way to expose temporal queries to applications

Temporal querying is only available through the Go API (`driver.AsOfReadTransaction`).
There is no query-language-level syntax for "query as of time T" — the application must
call the Go function directly, the way the demo script does.

Oracle solves this by extending SQL (`SELECT ... AS OF TIMESTAMP`). We could do something
similar by extending Cypher (e.g. `MATCH (n) AS OF $timestamp`) to lower into
`AsOfReadTransaction` during query planning. Until then, applications that want to expose
temporal queries to their users need to accept a timestamp parameter in their own API layer
and wire it through to the Go call.
