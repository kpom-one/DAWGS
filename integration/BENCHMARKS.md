# Integration Benchmarks

|                |              |
| -------------- | ------------ |
| **Git Ref**    | f181a6a      |
| **Date**       | 2026-04-01   |
| **Iterations** | 10           |

## base dataset (3 nodes)

### Match Nodes

| Driver | Median | P95 | Max |
|--------|-------:|----:|----:|
| sonic | 0.00ms | 0.00ms | 0.00ms |
| neo4j | 0.94ms | 1.1ms | 1.1ms |
| pg | 0.40ms | 1.7ms | 1.7ms |

### Match Edges

| Driver | Median | P95 | Max |
|--------|-------:|----:|----:|
| sonic | 0.00ms | 0.00ms | 0.00ms |
| neo4j | 1.1ms | 1.3ms | 1.3ms |
| pg | 60.8ms | 79.8ms | 79.8ms |

### Shortest Paths (n1 -> n3)

| Driver | Median | P95 | Max |
|--------|-------:|----:|----:|
| sonic | 0.08ms | 0.14ms | 0.14ms |
| neo4j | 1.4ms | 2.2ms | 2.2ms |
| pg | 402ms | 475ms | 475ms |

### Traversal (n1)

| Driver | Median | P95 | Max |
|--------|-------:|----:|----:|
| sonic | 0.06ms | 0.18ms | 0.18ms |
| neo4j | 1.2ms | 1.7ms | 1.7ms |
| pg | 392ms | 403ms | 403ms |

### Match Return (n1)

| Driver | Median | P95 | Max |
|--------|-------:|----:|----:|
| sonic | 0.06ms | 0.06ms | 0.06ms |
| neo4j | 1.2ms | 1.8ms | 1.8ms |
| pg | 1.0ms | 3.9ms | 3.9ms |

### Filter By Kind

| Driver | Kind | Median | P95 | Max |
|--------|------|-------:|----:|----:|
| sonic | NodeKind1 | 0.03ms | 0.03ms | 0.03ms |
| sonic | NodeKind2 | 0.03ms | 0.04ms | 0.04ms |
| neo4j | NodeKind1 | 1.2ms | 1.3ms | 1.3ms |
| neo4j | NodeKind2 | 1.1ms | 1.4ms | 1.4ms |
| pg | NodeKind1 | 0.57ms | 1.3ms | 1.3ms |
| pg | NodeKind2 | 0.57ms | 1.3ms | 1.3ms |

## local/phantom dataset (15,041 nodes, 826,150 edges)

### Match Nodes

| Driver | Median | P95 | Max |
|--------|-------:|----:|----:|
| sonic | 0.16ms | 0.20ms | 0.20ms |
| neo4j | 1.2ms | 1.4ms | 1.4ms |
| pg | 2.4ms | 5.9ms | 5.9ms |

### Match Edges

| Driver | Median | P95 | Max |
|--------|-------:|----:|----:|
| sonic | 6.2ms | 7.2ms | 7.2ms |
| neo4j | 1.2ms | 2.2ms | 2.2ms |
| pg | 273ms | 296ms | 296ms |

### Filter By Kind

| Driver | Kind | Median | P95 | Max |
|--------|------|-------:|----:|----:|
| sonic | User | 0.52ms | 0.57ms | 0.57ms |
| sonic | Group | 0.63ms | 0.66ms | 0.66ms |
| sonic | Computer | 0.51ms | 0.53ms | 0.53ms |
| neo4j | User | 2.5ms | 6.6ms | 6.6ms |
| neo4j | Group | 2.0ms | 5.6ms | 5.6ms |
| neo4j | Computer | 1.4ms | 1.8ms | 1.8ms |
| pg | User | 2.0ms | 3.0ms | 3.0ms |
| pg | Group | 2.2ms | 2.7ms | 2.7ms |
| pg | Computer | 1.2ms | 1.9ms | 1.9ms |

### Traversal Depth

| Driver | Depth | Median | P95 | Max |
|--------|------:|-------:|----:|----:|
| sonic | 1-3 | — | — | — |
| neo4j | 1 | 1.2ms | 1.5ms | 1.5ms |
| neo4j | 2 | 1.4ms | 1.8ms | 1.8ms |
| neo4j | 3 | 1.9ms | 2.6ms | 2.6ms |
| pg | 1 | 423ms | 428ms | 428ms |
| pg | 2 | 528ms | 562ms | 562ms |
| pg | 3 | 539ms | 552ms | 552ms |

> **Sonic:** Traversal Depth scenarios fail with `binding count exceeded 100000` — variable-length path queries expand beyond the current binding limit.

### Edge Kind Traversal

| Driver | Kind | Median | P95 | Max |
|--------|------|-------:|----:|----:|
| sonic | MemberOf | 96.9ms | 149ms | 149ms |
| sonic | GenericAll | 84.5ms | 114ms | 114ms |
| sonic | HasSession | 83.8ms | 135ms | 135ms |
| neo4j | MemberOf | 1.1ms | 1.6ms | 1.6ms |
| neo4j | GenericAll | 1.1ms | 1.9ms | 1.9ms |
| neo4j | HasSession | 1.1ms | 2.6ms | 2.6ms |
| pg | MemberOf | 504ms | 537ms | 537ms |
| pg | GenericAll | 489ms | 499ms | 499ms |
| pg | HasSession | 481ms | 515ms | 515ms |

### Shortest Paths (41 -> 587)

| Driver | Median | P95 | Max |
|--------|-------:|----:|----:|
| sonic | 0.07ms | 0.09ms | 0.09ms |
| neo4j | 1.6ms | 2.3ms | 2.3ms |
| pg | 691ms | 735ms | 735ms |
