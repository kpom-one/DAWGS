package drivers_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/sonic"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

var (
	User       = graph.StringKind("User")
	Group      = graph.StringKind("Group")
	Computer   = graph.StringKind("Computer")
	Domain     = graph.StringKind("Domain")
	MemberOf   = graph.StringKind("MemberOf")
	HasSession = graph.StringKind("HasSession")
	AdminTo    = graph.StringKind("AdminTo")
	GenericAll = graph.StringKind("GenericAll")

	nodeKinds = []graph.Kind{User, Group, Computer, Domain}
	edgeKinds = []graph.Kind{MemberOf, HasSession, AdminTo, GenericAll}
)

// graphSizes defines the number of edges to benchmark at each tier.
var graphSizes = []int{100_000, 1_000_000}

// benchGraph is the graph schema used for PG benchmarks.
var benchGraph = graph.Graph{
	Name:  "bench",
	Nodes: graph.Kinds{User, Group, Computer, Domain},
	Edges: graph.Kinds{MemberOf, HasSession, AdminTo, GenericAll},
}

// driverFactory creates a fresh graph.Database for benchmarking.
type driverFactory struct {
	name     string
	newFresh func() graph.Database
	cleanup  func()
}

func drivers() []driverFactory {
	if connStr := os.Getenv("PG_CONNECTION_STRING"); connStr != "" {
		return []driverFactory{newPGFactory(connStr)}
	}

	return []driverFactory{
		{
			name:     "sonic",
			newFresh: func() graph.Database { return sonic.NewDatabase() },
			cleanup:  func() {},
		},
	}
}

// populateGraph creates a random graph with the given number of edges.
func populateGraph(db graph.Database, numEdges int) ([]graph.ID, error) {
	ctx := context.Background()

	numNodes := numEdges / 3
	if numNodes < 10 {
		numNodes = 10
	}

	rng := rand.New(rand.NewSource(42))

	var nodeIDs []graph.ID

	// Create nodes
	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i := 0; i < numNodes; i++ {
			kind := nodeKinds[rng.Intn(len(nodeKinds))]
			props := graph.AsProperties(map[string]any{
				"name":    fmt.Sprintf("node-%d", i),
				"rank":    i,
				"created": time.Now().Unix(),
			})

			node, err := tx.CreateNode(props, kind)
			if err != nil {
				return err
			}
			nodeIDs = append(nodeIDs, node.ID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Generate unique edges (PG has a unique constraint on start_id, end_id, kind_id)
	type edgeKey struct {
		src, dst graph.ID
		kind     int
	}
	seen := make(map[edgeKey]struct{}, numEdges)

	err = db.BatchOperation(ctx, func(batch graph.Batch) error {
		for i := 0; i < numEdges; i++ {
			src := nodeIDs[rng.Intn(len(nodeIDs))]
			dst := nodeIDs[rng.Intn(len(nodeIDs))]
			for dst == src {
				dst = nodeIDs[rng.Intn(len(nodeIDs))]
			}
			kindIdx := rng.Intn(len(edgeKinds))

			key := edgeKey{src, dst, kindIdx}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}

			if err := batch.CreateRelationshipByIDs(src, dst, edgeKinds[kindIdx], graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	})

	return nodeIDs, err
}

// clearGraph deletes all nodes and edges from the database.
func clearGraph(db graph.Database) error {
	ctx := context.Background()
	return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if err := tx.Relationships().Delete(); err != nil {
			return err
		}
		return tx.Nodes().Delete()
	})
}

// rssMB shells out to ps to get the actual RSS of this process in MB.
func rssMB() float64 {
	pid := os.Getpid()
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	kb, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	return kb / 1024
}

// reportRSS reports absolute RSS as a custom metric.
func reportRSS(b *testing.B) {
	b.ReportMetric(rssMB(), "rss-MB")
}

// --- Benchmark Functions ---

// BenchmarkIngest measures raw node+edge creation throughput.
func BenchmarkIngest(b *testing.B) {
	for _, drv := range drivers() {
		for _, sz := range graphSizes {
			b.Run(fmt.Sprintf("%s/edges=%d", drv.name, sz), func(b *testing.B) {
				db := drv.newFresh()
				defer drv.cleanup()
				b.ResetTimer()

				for b.Loop() {
					if err := clearGraph(db); err != nil {
						b.Fatal(err)
					}
					if _, err := populateGraph(db, sz); err != nil {
						b.Fatal(err)
					}
				}
				reportRSS(b)
			})
		}
	}
}

// BenchmarkNodeCountByKind measures filtering and counting nodes by kind.
func BenchmarkNodeCountByKind(b *testing.B) {
	for _, drv := range drivers() {
		for _, sz := range graphSizes {
			b.Run(fmt.Sprintf("%s/edges=%d", drv.name, sz), func(b *testing.B) {
				db := drv.newFresh()
				defer drv.cleanup()
				if _, err := populateGraph(db, sz); err != nil {
					b.Fatal(err)
				}

				ctx := context.Background()
				b.ResetTimer()

				for b.Loop() {
					_ = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
						for _, kind := range nodeKinds {
							if _, err := tx.Nodes().Filter(query.KindIn(query.Node(), kind)).Count(); err != nil {
								return err
							}
						}
						return nil
					})
				}
			})
		}
	}
}

// BenchmarkShortestPath measures BFS shortest path between two nodes.
func BenchmarkShortestPath(b *testing.B) {
	for _, drv := range drivers() {
		for _, sz := range graphSizes {
			b.Run(fmt.Sprintf("%s/edges=%d", drv.name, sz), func(b *testing.B) {
				db := drv.newFresh()
				defer drv.cleanup()
				nodeIDs, err := populateGraph(db, sz)
				if err != nil {
					b.Fatal(err)
				}

				src := nodeIDs[0]
				dst := nodeIDs[len(nodeIDs)-1]
				ctx := context.Background()
				b.ResetTimer()

				for b.Loop() {
					_ = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
						return tx.Relationships().
							Filter(query.InIDs(query.StartID(), src)).
							Filter(query.InIDs(query.EndID(), dst)).
							FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
								for range cursor.Chan() {
								}
								return cursor.Error()
							})
					})
				}
			})
		}
	}
}

// BenchmarkFetchNodesByProperty measures property-based node lookup.
func BenchmarkFetchNodesByProperty(b *testing.B) {
	for _, drv := range drivers() {
		for _, sz := range graphSizes {
			b.Run(fmt.Sprintf("%s/edges=%d", drv.name, sz), func(b *testing.B) {
				db := drv.newFresh()
				defer drv.cleanup()
				if _, err := populateGraph(db, sz); err != nil {
					b.Fatal(err)
				}

				ctx := context.Background()
				rng := rand.New(rand.NewSource(99))
				numNodes := sz / 3
				if numNodes < 10 {
					numNodes = 10
				}
				b.ResetTimer()

				for b.Loop() {
					target := fmt.Sprintf("node-%d", rng.Intn(numNodes))
					_ = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
						_, err := tx.Nodes().Filter(
							query.Equals(query.NodeProperty("name"), target),
						).First()
						return err
					})
				}
			})
		}
	}
}

// BenchmarkRelationshipTraversal measures fetching all outbound edges from random nodes.
func BenchmarkRelationshipTraversal(b *testing.B) {
	for _, drv := range drivers() {
		for _, sz := range graphSizes {
			b.Run(fmt.Sprintf("%s/edges=%d", drv.name, sz), func(b *testing.B) {
				db := drv.newFresh()
				defer drv.cleanup()
				nodeIDs, err := populateGraph(db, sz)
				if err != nil {
					b.Fatal(err)
				}

				ctx := context.Background()
				rng := rand.New(rand.NewSource(77))
				b.ResetTimer()

				for b.Loop() {
					nodeID := nodeIDs[rng.Intn(len(nodeIDs))]
					_ = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
						return tx.Relationships().Filter(
							query.Equals(query.StartID(), nodeID),
						).Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
							for range cursor.Chan() {
							}
							return cursor.Error()
						})
					})
				}
			})
		}
	}
}

// BenchmarkKindFilterWithLimit measures fetching a limited set of nodes by kind.
func BenchmarkKindFilterWithLimit(b *testing.B) {
	for _, drv := range drivers() {
		for _, sz := range graphSizes {
			b.Run(fmt.Sprintf("%s/edges=%d", drv.name, sz), func(b *testing.B) {
				db := drv.newFresh()
				defer drv.cleanup()
				if _, err := populateGraph(db, sz); err != nil {
					b.Fatal(err)
				}

				ctx := context.Background()
				b.ResetTimer()

				for b.Loop() {
					_ = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
						return tx.Nodes().
							Filter(query.KindIn(query.Node(), User)).
							Limit(10).
							Fetch(func(cursor graph.Cursor[*graph.Node]) error {
								for range cursor.Chan() {
								}
								return cursor.Error()
							})
					})
				}
			})
		}
	}
}

// NOTE: BenchmarkBatchUpsert was removed. Sonic's UpdateNodeBy/UpdateRelationshipBy
// do a full O(N) scan per call to match identity properties, making them O(N²) for
// bulk upserts. At >10K nodes this is unusable. See TODO in sonic/batch.go.
// Re-add this benchmark once sonic has a secondary index on identity properties.
