# Sonic — In-Memory DAWGS Graph Driver

Sonic is an in-memory graph database driver for DAWGS. It implements the same `graph.Database` interface as the `pg` driver, giving BloodHound users a zero-infrastructure option with no Postgres required.

`sonic` because it's really fast — no network, no disk, no MVCC overhead.

## Architecture

All data lives in Go maps protected by a `sync.RWMutex`. Adjacency indexes (`outEdges`, `inEdges`) map node IDs to their edge IDs for O(1) neighbor lookup. IDs are assigned via `atomic.Uint64`.

The driver registers itself as `"sonic"` via `dawgs.Register()` in `init()`, following the same pattern as the `pg` driver.

## Files

| File | Purpose |
|------|---------|
| `sonic.go` | `Database` struct, constructor, `graph.Database` interface |
| `transaction.go` | `graph.Transaction` — node/edge CRUD, Cypher dispatch |
| `batch.go` | `graph.Batch` — bulk CRUD with upsert support |
| `queries.go` | `graph.NodeQuery` / `graph.RelationshipQuery` — filtering, fetching, shortest paths |
| `eval.go` | Cypher AST filter evaluation, comparison operators, type coercion |
| `pathfinding.go` | BFS shortest-path algorithm with constraint extraction |
| `cypher.go` | Cypher AST walker — MATCH, WITH, RETURN, WHERE, variable-length paths |
| `execute.go` | `sonicResult` — result set iteration, scanning, value mapping |

## What Works

### Graph Operations (via `graph.*` interfaces)

- **CRUD**: CreateNode, UpdateNode, DeleteNode, CreateRelationship, CreateRelationshipByIDs, UpdateRelationship, DeleteRelationship
- **Node queries**: Filter, Filterf, First, Count, Fetch, FetchIDs, FetchKinds, Delete, Update, Query
- **Relationship queries**: Filter, Filterf, First, Count, Fetch, FetchIDs, FetchKinds, FetchDirection, FetchTriples, FetchAllShortestPaths, Delete, Update, Query
- **Batch upserts**: UpdateNodeBy, UpdateRelationshipBy (identity-based match/create/update)
- **Schema**: AssertSchema, SetDefaultGraph, FetchKinds

### Filter Evaluation

The driver evaluates the Cypher AST that DAWGS query builders produce:

- **Logical**: Conjunction (AND), Disjunction (OR), Negation (NOT), Parenthetical
- **Comparisons**: `=`, `!=`, `<`, `>`, `<=`, `>=`, `IN`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `IS NULL`, `IS NOT NULL`
- **Kind matching**: node kinds, edge kinds, start/end node kinds
- **Functions**: `id()`, `type()`, `toLower()`, `toUpper()`, `labels()`, `keys()`
- **Property resolution**: node/edge properties, start/end node properties via `query.EdgeStartSymbol`/`query.EdgeEndSymbol`

### Cypher Execution

Raw Cypher strings are parsed and executed via an AST walker:

- MATCH / OPTIONAL MATCH with node and relationship patterns
- WHERE clause filtering with full expression evaluation
- WITH (scope barriers, projection, aggregation aliases)
- RETURN (*, named projections)
- ORDER BY, LIMIT, SKIP, DISTINCT
- `allShortestPaths()` pattern
- Variable-length relationship patterns (`[*]`, `[*1..3]`)
- Multi-part queries (multiple MATCH/WITH chains)
- Parameter substitution

### Pathfinding

BFS shortest-path implementation that:
- Finds **all** equally-short paths between start and end nodes
- Respects edge kind constraints
- Supports multiple start/end nodes simultaneously
- Uses bidirectional parent tracking for path reconstruction

## What's Not Supported

- **Cypher write operations**: CREATE, DELETE, SET, REMOVE, MERGE return errors. Use the `graph.Transaction` or `graph.Batch` interfaces for writes.
- **UNWIND, quantifiers, filter expressions** in Cypher
- **Aggregation functions** (count, collect, sum, avg, min, max) — return nil stubs in Cypher evaluation
- **OrderBy, Offset, Limit** on `nodeQuery`/`relQuery` — accepted but no-op
- **Persistence** — data lives only in memory, lost on process exit (by design)

## Benchmarks (Apple M3 Max)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SONIC DRIVER BENCHMARKS (Apple M3 Max)                    │
├─────────────────────┬──────────┬──────────┬──────────┬──────────────────────┤
│ Benchmark           │   100K   │    1M    │    5M    │   10M edges          │
├─────────────────────┼──────────┼──────────┼──────────┼──────────────────────┤
│ Ingest              │    52ms  │   774ms  │   4.9s   │  10.6s               │
│ RSS after ingest    │    94MB  │   559MB  │   2.5GB  │   4.9GB              │
├─────────────────────┼──────────┼──────────┼──────────┼──────────────────────┤
│ Count by Kind       │   2.9ms  │    45ms  │   447ms  │   968ms              │
│ Shortest Path       │   857ns  │   238ms  │   447ms  │   1.8s               │
│ Fetch by Property   │   5.8ms  │   171ms  │   1.1s   │   1.9s               │
│ Rel Traversal       │   5.3ms  │    68ms  │   818ms  │   2.7s               │
│ Cypher (LIMIT 10)   │   2.6ms  │    27ms  │    43ms* │    51ms*             │
├─────────────────────┴──────────┴──────────┴──────────┴──────────────────────┤
│ * = binding limit errors (100K cap, LIMIT not pushed down)                  │
│                                                                             │
│ Strengths: Fast ingest, sub-μs traversal from known nodes                   │
│ Weakness:  No secondary indexes — all filters are O(N) full scans           │
│ Memory:    ~500 bytes/edge all-in                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

Run benchmarks: `go test -run='^$' -bench=. -benchtime=100ms ./drivers/`

## Constraints

- **No persistence** — data is lost when the process exits. By design for the initial version.
- **Coarse locking** — `sync.RWMutex` protects the whole database, not individual operations. Fine for single-user BHE.
- **Non-deterministic ordering** — map iteration means query results may come back in different orders than Postgres.
- **Binding limit** — Cypher execution caps at 100,000 intermediate bindings.
- **Variable-length path depth** — capped at 50 hops with cycle prevention.

## Tests

- **Unit tests** (`sonic_test.go`): CRUD, property filters, shortest paths, Cypher queries (kind filtering, negation, multi-part, variable-length paths, anonymous nodes, concurrent access)
- **Integration tests** (`integration_test.go`): node/relationship operations, attack path finding, batch upserts, parallel fetches against a realistic graph topology
