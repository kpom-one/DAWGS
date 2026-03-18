# Temporal Pathfinding Benchmarks

Machine: Apple M3 Max, Postgres local, default config (`shared_buffers=128MB`, `temp_buffers=8MB`).

Query: shortest path, 6 hops, through a graph of random noise edges.

| Graph size              | ReadTransaction | AsOfReadTransaction | Overhead          |
| ----------------------- | --------------- | ------------------- | ----------------- |
| 10k nodes / 50k edges   | 745ms           | 405ms               | **-46% (faster)** |
| 100k nodes / 500k edges | 1,100ms         | 1,200ms             | **+10%**          |

At small scale, asOf is faster because temp tables skip Postgres MVCC visibility checks. At larger scale, the reconstruction cost catches up and asOf is ~10% slower.

Setup: 6-hop attack path (User → Group × 4 → Computer → DomainAdmin) embedded in random noise. Post-mark churn: 10k edges added, 5k deleted.

Run:

```
PG_CONNECTION_STRING=... go test -tags manual_integration -run='^$' -bench=BenchmarkAsOf -count=10 -v ./drivers/pg/
```
