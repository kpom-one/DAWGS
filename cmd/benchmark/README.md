# Benchmark

Runs query scenarios against a real database and outputs a markdown timing table.

## Usage

```bash
# PostgreSQL — base dataset
go run ./cmd/benchmark -driver pg -connection "postgresql://dawgs:dawgs@localhost:5432/dawgs" -dataset base

# Neo4j — base dataset
go run ./cmd/benchmark -driver neo4j -connection "neo4j://neo4j:testpassword@localhost:7687" -dataset base

# Sonic — base dataset (no connection string needed)
go run ./cmd/benchmark -driver sonic -dataset base

# Local dataset (not committed to repo)
go run ./cmd/benchmark -driver sonic -dataset local/phantom

# Default + local dataset
go run ./cmd/benchmark -connection "..." -local-dataset local/phantom

# Save to file
go run ./cmd/benchmark -connection "..." -output report.md
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-driver` | `pg` | Database driver (`pg`, `neo4j`, `sonic`) |
| `-connection` | | Connection string (or `PG_CONNECTION_STRING` env). Not required for sonic. |
| `-iterations` | `10` | Timed iterations per scenario |
| `-dataset` | | Run only this dataset |
| `-local-dataset` | | Add a local dataset to the default set |
| `-dataset-dir` | `integration/testdata` | Path to testdata directory |
| `-output` | stdout | Markdown output file |

## Results — base dataset (3 nodes)

### Sonic

| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | base | 0.00ms | 0.00ms | 0.00ms |
| Match Edges | base | 0.00ms | 0.00ms | 0.00ms |
| Shortest Paths / n1 -> n3 | base | 0.08ms | 0.14ms | 0.14ms |
| Traversal / n1 | base | 0.06ms | 0.18ms | 0.18ms |
| Match Return / n1 | base | 0.06ms | 0.06ms | 0.06ms |
| Filter By Kind / NodeKind1 | base | 0.03ms | 0.03ms | 0.03ms |
| Filter By Kind / NodeKind2 | base | 0.03ms | 0.04ms | 0.04ms |

### Neo4j

| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | base | 0.94ms | 1.1ms | 1.1ms |
| Match Edges | base | 1.1ms | 1.3ms | 1.3ms |
| Shortest Paths / n1 -> n3 | base | 1.4ms | 2.2ms | 2.2ms |
| Traversal / n1 | base | 1.2ms | 1.7ms | 1.7ms |
| Match Return / n1 | base | 1.2ms | 1.8ms | 1.8ms |
| Filter By Kind / NodeKind1 | base | 1.2ms | 1.3ms | 1.3ms |
| Filter By Kind / NodeKind2 | base | 1.1ms | 1.4ms | 1.4ms |

### PostgreSQL

| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | base | 0.40ms | 1.7ms | 1.7ms |
| Match Edges | base | 60.8ms | 79.8ms | 79.8ms |
| Shortest Paths / n1 -> n3 | base | 402ms | 475ms | 475ms |
| Traversal / n1 | base | 392ms | 403ms | 403ms |
| Match Return / n1 | base | 1.0ms | 3.9ms | 3.9ms |
| Filter By Kind / NodeKind1 | base | 0.57ms | 1.3ms | 1.3ms |
| Filter By Kind / NodeKind2 | base | 0.57ms | 1.3ms | 1.3ms |

## Results — local/phantom dataset (15,041 nodes, 826,150 edges)

### Sonic

| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | local/phantom | 0.16ms | 0.20ms | 0.20ms |
| Match Edges | local/phantom | 6.2ms | 7.2ms | 7.2ms |
| Filter By Kind / User | local/phantom | 0.52ms | 0.57ms | 0.57ms |
| Filter By Kind / Group | local/phantom | 0.63ms | 0.66ms | 0.66ms |
| Filter By Kind / Computer | local/phantom | 0.51ms | 0.53ms | 0.53ms |
| Edge Kind Traversal / MemberOf | local/phantom | 96.9ms | 149ms | 149ms |
| Edge Kind Traversal / GenericAll | local/phantom | 84.5ms | 114ms | 114ms |
| Edge Kind Traversal / HasSession | local/phantom | 83.8ms | 135ms | 135ms |
| Shortest Paths / 41 -> 587 | local/phantom | 0.07ms | 0.09ms | 0.09ms |

> **Note:** Traversal Depth scenarios (depth 1-3) fail on sonic with `binding count exceeded 100000` — the variable-length path queries expand beyond sonic's current binding limit.

### Neo4j

| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | local/phantom | 1.2ms | 1.4ms | 1.4ms |
| Match Edges | local/phantom | 1.2ms | 2.2ms | 2.2ms |
| Filter By Kind / User | local/phantom | 2.5ms | 6.6ms | 6.6ms |
| Filter By Kind / Group | local/phantom | 2.0ms | 5.6ms | 5.6ms |
| Filter By Kind / Computer | local/phantom | 1.4ms | 1.8ms | 1.8ms |
| Traversal Depth / depth 1 | local/phantom | 1.2ms | 1.5ms | 1.5ms |
| Traversal Depth / depth 2 | local/phantom | 1.4ms | 1.8ms | 1.8ms |
| Traversal Depth / depth 3 | local/phantom | 1.9ms | 2.6ms | 2.6ms |
| Edge Kind Traversal / MemberOf | local/phantom | 1.1ms | 1.6ms | 1.6ms |
| Edge Kind Traversal / GenericAll | local/phantom | 1.1ms | 1.9ms | 1.9ms |
| Edge Kind Traversal / HasSession | local/phantom | 1.1ms | 2.6ms | 2.6ms |
| Shortest Paths / 41 -> 587 | local/phantom | 1.6ms | 2.3ms | 2.3ms |

### PostgreSQL

| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | local/phantom | 2.4ms | 5.9ms | 5.9ms |
| Match Edges | local/phantom | 273ms | 296ms | 296ms |
| Filter By Kind / User | local/phantom | 2.0ms | 3.0ms | 3.0ms |
| Filter By Kind / Group | local/phantom | 2.2ms | 2.7ms | 2.7ms |
| Filter By Kind / Computer | local/phantom | 1.2ms | 1.9ms | 1.9ms |
| Traversal Depth / depth 1 | local/phantom | 423ms | 428ms | 428ms |
| Traversal Depth / depth 2 | local/phantom | 528ms | 562ms | 562ms |
| Traversal Depth / depth 3 | local/phantom | 539ms | 552ms | 552ms |
| Edge Kind Traversal / MemberOf | local/phantom | 504ms | 537ms | 537ms |
| Edge Kind Traversal / GenericAll | local/phantom | 489ms | 499ms | 499ms |
| Edge Kind Traversal / HasSession | local/phantom | 481ms | 515ms | 515ms |
| Shortest Paths / 41 -> 587 | local/phantom | 691ms | 735ms | 735ms |
