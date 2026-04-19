//go:build manual_integration

package pg_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/pg/temporaltest"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// logPath pretty-prints a path for readable test output.
func logPath(t *testing.T, label string, path graph.Path) {
	t.Helper()

	var parts []string
	path.Walk(func(start, end *graph.Node, rel *graph.Relationship) bool {
		if len(parts) == 0 {
			startName, _ := start.Properties.Get("name").String()
			parts = append(parts, fmt.Sprintf("(%s:%s)", startName, strings.Join(start.Kinds.Strings(), ":")))
		}
		endName, _ := end.Properties.Get("name").String()
		parts = append(parts, fmt.Sprintf("-[%s]-> (%s:%s)", rel.Kind.String(), endName, strings.Join(end.Kinds.Strings(), ":")))
		return true
	})

	t.Logf("[%s] Path (len=%d): %s", label, len(path.Edges), strings.Join(parts, " "))
}

// TestAsOfReadTransaction_SeesHistoricalState verifies that new edges created after the
// marked time are not visible in a historical query.
func TestAsOfReadTransaction_SeesHistoricalState(t *testing.T) {
	env := temporaltest.New(t)
	defer env.Close()

	userID := env.CreateNode("UserA", temporaltest.KindUser)
	groupID := env.CreateNode("SecurityGroup", temporaltest.KindGroup)
	computerID := env.CreateNode("WorkstationX", temporaltest.KindComputer)
	domainAdminID := env.CreateNode("DomainAdmin", temporaltest.KindDomainAdmin)

	// Initial state: only User -> Group
	env.CreateEdge(userID, groupID, temporaltest.KindMemberOf)

	mark := env.Mark()

	// After mark: add edges that create an attack path
	env.CreateEdge(groupID, computerID, temporaltest.KindAdminTo)
	env.CreateEdge(computerID, domainAdminID, temporaltest.KindHasSession)

	// Current state has 3 edges
	env.AssertCurrentEdgeCount(3)

	// Historical state should only see 1 edge
	require.NoError(t, env.Driver.AsOfReadTransaction(env.Ctx(), mark, func(tx graph.Transaction) error {
		count, err := tx.Relationships().Count()
		require.NoError(t, err)
		require.Equal(t, int64(1), count, "historical graph should have only 1 edge (MemberOf)")

		// Verify it's the MemberOf edge
		rel, err := tx.Relationships().Filter(query.Kind(query.Relationship(), temporaltest.KindMemberOf)).First()
		require.NoError(t, err)
		require.Equal(t, userID, rel.StartID)
		require.Equal(t, groupID, rel.EndID)

		// AdminTo should not exist yet
		adminCount, err := tx.Relationships().Filter(query.Kind(query.Relationship(), temporaltest.KindAdminTo)).Count()
		require.NoError(t, err)
		require.Equal(t, int64(0), adminCount, "AdminTo edge should not exist at marked time")

		return nil
	}))
}

// TestAsOfReadTransaction_SeesDeletedEdges verifies that edges deleted after the marked
// time are still visible in a historical query.
func TestAsOfReadTransaction_SeesDeletedEdges(t *testing.T) {
	env := temporaltest.New(t)
	defer env.Close()

	userID := env.CreateNode("UserA", temporaltest.KindUser)
	groupID := env.CreateNode("SecurityGroup", temporaltest.KindGroup)

	memberOfRelID := env.CreateEdge(userID, groupID, temporaltest.KindMemberOf)

	mark := env.Mark()

	// Delete the edge
	env.DeleteEdgeByID(memberOfRelID)

	env.AssertCurrentEdgeCount(0)

	// Historical state should still see the deleted edge
	require.NoError(t, env.Driver.AsOfReadTransaction(env.Ctx(), mark, func(tx graph.Transaction) error {
		count, err := tx.Relationships().Count()
		require.NoError(t, err)
		require.Equal(t, int64(1), count, "historical graph should still have the deleted edge")

		rel, err := tx.Relationships().First()
		require.NoError(t, err)
		require.Equal(t, userID, rel.StartID)
		require.Equal(t, groupID, rel.EndID)

		return nil
	}))
}

// TestAsOfReadTransaction_NodesAlsoHistorical verifies that nodes created after the marked
// time are not visible in historical queries.
func TestAsOfReadTransaction_NodesAlsoHistorical(t *testing.T) {
	env := temporaltest.New(t)
	defer env.Close()

	env.CreateNode("UserA", temporaltest.KindUser)

	mark := env.Mark()

	env.CreateNode("UserB", temporaltest.KindUser)
	env.CreateNode("UserC", temporaltest.KindUser)

	env.AssertCurrentNodeCount(3)
	env.AssertHistoricalNodeCount(mark, 1)
}

// TestTemporalAttackPathAppears — "The Intern Gets Promoted"
//
// jsmith is a regular user. A chain exists from a privileged group through a domain controller
// to a DA account, but jsmith isn't connected to it. After the mark, jsmith gets added to
// the privileged group — completing the attack path.
func TestTemporalAttackPathAppears(t *testing.T) {
	env := temporaltest.New(t)
	defer env.Close()

	jsmithID := env.CreateNode("jsmith", temporaltest.KindUser)
	regularUsersID := env.CreateNode("Regular Users", temporaltest.KindGroup)
	domainAdminsID := env.CreateNode("Domain Admins Group", temporaltest.KindGroup)
	dc01ID := env.CreateNode("DC01", temporaltest.KindComputer)
	dasvcID := env.CreateNode("DA-SVC", temporaltest.KindDomainAdmin)

	// jsmith -> Regular Users (harmless)
	env.CreateEdge(jsmithID, regularUsersID, temporaltest.KindMemberOf)
	// Domain Admins Group -> DC01
	env.CreateEdge(domainAdminsID, dc01ID, temporaltest.KindAdminTo)
	// DC01 -> DA-SVC
	env.CreateEdge(dc01ID, dasvcID, temporaltest.KindHasSession)

	// Mark: no path from jsmith to DA-SVC at this point
	mark := env.Mark()

	// The accident — jsmith added to Domain Admins Group
	env.CreateEdge(jsmithID, domainAdminsID, temporaltest.KindMemberOf)

	// Current state: attack path exists (3 hops)
	require.NoError(t, env.Driver.ReadTransaction(env.Ctx(), func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), jsmithID),
				query.Equals(query.EndID(), dasvcID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				require.Equal(t, 3, len(path.Edges), "attack path should be 3 hops")
				logPath(t, "CURRENT", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.True(t, found, "current state should have attack path from jsmith to DA-SVC")
		return nil
	}))

	// Historical state: no path from jsmith to DA-SVC
	require.NoError(t, env.Driver.AsOfReadTransaction(env.Ctx(), mark, func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), jsmithID),
				query.Equals(query.EndID(), dasvcID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				logPath(t, "HISTORICAL (unexpected!)", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.False(t, found, "historical state should NOT have attack path from jsmith to DA-SVC")
		return nil
	}))
}

// TestTemporalAttackPathRemediated — "The Breach That Was Fixed"
//
// A full attack path exists through a compromised session. Security ops kill the session
// (delete the HasSession edge). Current state: path gone. Historical state: path still
// visible — proving the exposure window.
func TestTemporalAttackPathRemediated(t *testing.T) {
	env := temporaltest.New(t)
	defer env.Close()

	compromisedUserID := env.CreateNode("compromised_user", temporaltest.KindUser)
	itAdminsID := env.CreateNode("IT Admins", temporaltest.KindGroup)
	fileSrvID := env.CreateNode("FILE-SRV-01", temporaltest.KindComputer)
	daAdminID := env.CreateNode("DA-ADMIN", temporaltest.KindDomainAdmin)

	// Full attack path: user -> group -> computer -> DA
	env.CreateEdge(compromisedUserID, itAdminsID, temporaltest.KindMemberOf)
	env.CreateEdge(itAdminsID, fileSrvID, temporaltest.KindAdminTo)
	hasSessionRelID := env.CreateEdge(fileSrvID, daAdminID, temporaltest.KindHasSession)

	// Verify attack path exists before remediation
	require.NoError(t, env.Driver.ReadTransaction(env.Ctx(), func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), compromisedUserID),
				query.Equals(query.EndID(), daAdminID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				require.Equal(t, 3, len(path.Edges), "full attack path should be 3 hops")
				logPath(t, "BEFORE REMEDIATION", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.True(t, found, "attack path should exist before remediation")
		return nil
	}))

	// Mark: path exists at this point
	mark := env.Mark()

	// Security ops kill the session
	env.DeleteEdgeByID(hasSessionRelID)

	// Current state: no path (remediated)
	require.NoError(t, env.Driver.ReadTransaction(env.Ctx(), func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), compromisedUserID),
				query.Equals(query.EndID(), daAdminID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				logPath(t, "CURRENT (unexpected!)", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.False(t, found, "current state should NOT have attack path after remediation")
		return nil
	}))

	// Historical state: path still visible — proving the exposure window
	require.NoError(t, env.Driver.AsOfReadTransaction(env.Ctx(), mark, func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), compromisedUserID),
				query.Equals(query.EndID(), daAdminID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				require.Equal(t, 3, len(path.Edges), "historical attack path should be 3 hops")
				logPath(t, "HISTORICAL", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.True(t, found, "historical state should still show the attack path (exposure window)")
		return nil
	}))
}
