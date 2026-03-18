//go:build manual_integration

package pg_test

import (
	"testing"

	"github.com/specterops/dawgs/drivers/pg/temporaltest"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// BenchmarkAsOfPathfinding answers: "How much slower is pathfinding via AsOfReadTransaction?"
//
// Setup: build a chain of nodes (attacker -> group -> ... -> target), mark time, then add
// noise edges and delete some existing ones so reconstruction has real work to do.
// Measured: shortest-path query via normal read vs historical read.
//
// Run:
//
//	go test -tags manual_integration -run='^$' -bench=BenchmarkAsOf -count=10 -v ./drivers/pg/...
func BenchmarkAsOfPathfinding(b *testing.B) {
	env := temporaltest.New(b)
	defer env.Close()

	// Create noise first so the attack path edges aren't in the deletable pool.
	env.CreateNodes(100000)
	env.CreateRandomEdges(500000)

	// Build a 6-hop attack path: attacker -> g1 -> g2 -> g3 -> g4 -> server -> target
	attacker := env.CreateNode("attacker", temporaltest.KindUser)
	g1 := env.CreateNode("group-1", temporaltest.KindGroup)
	g2 := env.CreateNode("group-2", temporaltest.KindGroup)
	g3 := env.CreateNode("group-3", temporaltest.KindGroup)
	g4 := env.CreateNode("group-4", temporaltest.KindGroup)
	server := env.CreateNode("server", temporaltest.KindComputer)
	target := env.CreateNode("target", temporaltest.KindDomainAdmin)

	env.CreateEdge(attacker, g1, temporaltest.KindMemberOf)
	env.CreateEdge(g1, g2, temporaltest.KindMemberOf)
	env.CreateEdge(g2, g3, temporaltest.KindMemberOf)
	env.CreateEdge(g3, g4, temporaltest.KindMemberOf)
	env.CreateEdge(g4, server, temporaltest.KindAdminTo)
	env.CreateEdge(server, target, temporaltest.KindHasSession)

	mark := env.Mark()

	// Post-mark churn: add and delete edges so reconstruction is non-trivial.
	env.CreateRandomEdges(10000)
	env.DeleteEdges(5000)

	// Verify the path exists in both views before benchmarking.
	findPath := func(tx graph.Transaction) int {
		var count int
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), attacker),
				query.Equals(query.EndID(), target),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for range cursor.Chan() {
				count++
			}
			return cursor.Error()
		})
		require.NoError(b, err)
		return count
	}

	var currentPaths, historicalPaths int
	env.Driver.ReadTransaction(env.Ctx(), func(tx graph.Transaction) error {
		currentPaths = findPath(tx)
		return nil
	})
	env.Driver.AsOfReadTransaction(env.Ctx(), mark, func(tx graph.Transaction) error {
		historicalPaths = findPath(tx)
		return nil
	})

	b.Logf("paths found — current: %d, historical: %d", currentPaths, historicalPaths)
	require.Greater(b, currentPaths, 0, "should find at least one path in current state")
	require.Greater(b, historicalPaths, 0, "should find at least one path in historical state")

	b.Run("current", func(b *testing.B) {
		for b.Loop() {
			env.Driver.ReadTransaction(env.Ctx(), func(tx graph.Transaction) error {
				findPath(tx)
				return nil
			})
		}
	})

	b.Run("asOf", func(b *testing.B) {
		for b.Loop() {
			env.Driver.AsOfReadTransaction(env.Ctx(), mark, func(tx graph.Transaction) error {
				findPath(tx)
				return nil
			})
		}
	})
}
