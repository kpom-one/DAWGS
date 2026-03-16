//go:build manual_integration

package pg_test

import (
	"testing"

	"github.com/specterops/dawgs/drivers/pg/temporaltest"
	"github.com/stretchr/testify/require"
)

// BenchmarkReconstruction — "Can We Rebuild It?"
//
// The temporal system reconstructs historical graph state by creating temp tables that union
// current data with deletion logs. This benchmark answers: how fast is that reconstruction
// as the graph grows?
//
// Setup: create a graph, mark a point in time, then mutate (add and delete edges).
// Measured: reconstruct the graph as it was at the marked time, and verify the count is exact.
//
// Run with:
//
//	go test -tags manual_integration -bench=BenchmarkReconstruction -benchmem ./drivers/pg/...
//
// Compare across changes with:
//
//	go test -tags manual_integration -bench=BenchmarkReconstruction -count=5 > old.txt
//	# make changes
//	go test -tags manual_integration -bench=BenchmarkReconstruction -count=5 > new.txt
//	benchstat old.txt new.txt
func BenchmarkReconstruction(b *testing.B) {
	sizes := []struct {
		name              string
		nodes, edges      int
		deletes, newEdges int
	}{
		{"10k_50k", 10000, 50000, 5000, 10000},
		{"50k_250k", 50000, 250000, 25000, 50000},
		{"100k_500k", 100000, 500000, 50000, 100000},
		// The large sizes below will only run once by default (~2s and ~10s per op).
		// Use -benchtime=3x to force multiple iterations for a better average.
		{"200k_1M", 200000, 1000000, 100000, 200000},
		{"1M_5M", 1000000, 5000000, 500000, 1000000},
	}

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			env := temporaltest.New(b)
			defer env.Close()

			// Build the initial graph — this is the state we want to reconstruct later.
			env.CreateNodes(sz.nodes)
			env.CreateRandomEdges(sz.edges)
			expected := env.CurrentEdgeCount()

			// Mark the time. Everything after this is "the future" from the query's perspective.
			mark := env.Mark()

			// Mutate: add new edges and delete some old ones. This is the churn that the
			// reconstruction has to see through — new edges must be excluded, deleted edges
			// must be recovered from the deletion log.
			env.CreateRandomEdges(sz.newEdges)
			env.DeleteEdges(sz.deletes)

			// Only measure reconstruction, not setup.
			b.ResetTimer()
			for b.Loop() {
				got := env.HistoricalEdgeCount(mark)
				require.Equal(b, expected, got, "historical count must match pre-mutation count")
			}
		})
	}
}

// BenchmarkDeletionLogRecovery — "Everything Is Gone, Can We Get It Back?"
//
// Worst-case scenario for the deletion log: every edge that existed at the marked time has
// since been deleted. The historical view must recover all of them entirely from the
// edge_deletion_log table — zero edges come from the current edge table.
//
// This is the upper bound on reconstruction cost. If this is fast enough, everything else
// will be too.
func BenchmarkDeletionLogRecovery(b *testing.B) {
	env := temporaltest.New(b)
	defer env.Close()

	// Build a graph and record what it looks like.
	env.CreateNodes(10000)
	env.CreateRandomEdges(50000)
	expected := env.CurrentEdgeCount()

	// Mark the time while all edges still exist.
	mark := env.Mark()

	// Delete everything. The current graph is now empty.
	env.DeleteEdges(int(expected))
	env.AssertCurrentEdgeCount(0)

	// Reconstruction must recover every single edge from the deletion log.
	b.ResetTimer()
	for b.Loop() {
		got := env.HistoricalEdgeCount(mark)
		require.Equal(b, expected, got, "all deleted edges must be recovered from the log")
	}
}
