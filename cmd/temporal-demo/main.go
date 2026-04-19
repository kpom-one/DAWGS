// temporal-demo is a narrative demonstration of DAWGS temporal graph queries.
//
// It builds a small Active Directory-like topology, marks a point in time, applies
// specific changes that create new attack paths, then walks through the forensic
// analysis workflow: find new paths, identify which changes caused them, rank
// changes by impact, and simulate remediation.
//
// Usage:
//
//	export PG_CONNECTION_STRING="postgresql://dawgs:dawgs@localhost:5432/dawgs"
//	go run ./cmd/temporal-demo/
//
// The demo pauses between phases — press Enter to advance.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"
)

// AD-like kind definitions
var (
	kindUser        = graph.StringKind("User")
	kindGroup       = graph.StringKind("Group")
	kindComputer    = graph.StringKind("Computer")
	kindDomainAdmin = graph.StringKind("DomainAdmin")

	kindMemberOf   = graph.StringKind("MemberOf")
	kindAdminTo    = graph.StringKind("AdminTo")
	kindHasSession = graph.StringKind("HasSession")
)

// demo holds all state for the demonstration.
type demo struct {
	driver *pg.Driver
	db     graph.Database
	ctx    context.Context

	// Named node references for readable output
	nodes map[string]graph.ID
}

func main() {
	connStr := os.Getenv("PG_CONNECTION_STRING")
	if connStr == "" {
		log.Fatal("PG_CONNECTION_STRING must be set")
	}

	ctx := context.Background()
	pool, err := pg.NewPool(connStr)
	if err != nil {
		log.Fatal(err)
	}

	db, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		Pool:                  pool,
	})
	if err != nil {
		log.Fatal(err)
	}

	driver := db.(*pg.Driver)

	if err := db.AssertSchema(ctx, graph.Schema{
		Graphs: []graph.Graph{{
			Name:  "temporal_demo",
			Nodes: graph.Kinds{kindUser, kindGroup, kindComputer, kindDomainAdmin},
			Edges: graph.Kinds{kindMemberOf, kindAdminTo, kindHasSession},
		}},
		DefaultGraph: graph.Graph{Name: "temporal_demo"},
	}); err != nil {
		log.Fatal(err)
	}

	d := &demo{
		driver: driver,
		db:     db,
		ctx:    ctx,
		nodes:  make(map[string]graph.ID),
	}

	// Clean up any data from a previous run
	d.resetData()
	defer d.cleanup()

	d.run()
}

func (d *demo) run() {
	targets := []string{"DA-SVC", "DA-ADMIN"}
	attackers := []string{"jsmith", "jdoe", "contractor-7", "svc-backup"}

	// ---- Phase 1: Build the topology ----
	phase("Phase 1: Building enterprise topology")

	// Users
	d.createNode("jsmith", kindUser)
	d.createNode("jdoe", kindUser)
	d.createNode("contractor-7", kindUser)
	d.createNode("svc-backup", kindUser)

	// Groups
	d.createNode("Domain Users", kindGroup)
	d.createNode("IT Admins", kindGroup)
	d.createNode("Contractors", kindGroup)
	d.createNode("Domain Admins", kindGroup)

	// Computers
	d.createNode("WS-001", kindComputer)
	d.createNode("FILE-SRV-01", kindComputer)
	d.createNode("DC01", kindComputer)

	// Domain Admin accounts
	d.createNode("DA-SVC", kindDomainAdmin)
	d.createNode("DA-ADMIN", kindDomainAdmin)

	// Benign memberships
	d.createEdge("jsmith", "Domain Users", kindMemberOf)
	d.createEdge("jdoe", "Domain Users", kindMemberOf)
	d.createEdge("contractor-7", "Contractors", kindMemberOf)
	d.createEdge("svc-backup", "IT Admins", kindMemberOf)

	// Existing admin relationships
	d.createEdge("IT Admins", "FILE-SRV-01", kindAdminTo)
	d.createEdge("Domain Admins", "DC01", kindAdminTo)

	// Existing sessions
	d.createEdge("DC01", "DA-SVC", kindHasSession)

	d.printTopology()

	// ---- Phase 2: Scan for attack paths ----
	phase("Phase 2: Scanning for attack paths")

	fmt.Println("  An attack path is any chain of relationships (MemberOf, AdminTo, HasSession)")
	fmt.Println("  that connects a regular user to a Domain Admin account. If one exists, that")
	fmt.Println("  user can escalate privileges to full domain control.")
	fmt.Println()

	pathsBefore := d.findAttackPaths(attackers, targets)
	if len(pathsBefore) > 0 {
		fmt.Printf("  Found %d attack path(s):\n", len(pathsBefore))
		for _, p := range pathsBefore {
			d.printPath("    ", p)
		}
	} else {
		fmt.Println("  No attack paths found. The environment is clean.")
	}

	// ---- Phase 3: Simulate the next day's ingestion ----
	// Mark time silently — we'll explain it when it becomes relevant.
	mark := d.mark()

	phase("Phase 3: Simulating changes (the next day's ingestion)")

	// Noisy changes that don't contribute to any attack path
	d.createNode("PRINT-SRV", kindComputer)
	fmt.Println("  Change  1: New computer PRINT-SRV added")
	d.createEdge("Domain Users", "PRINT-SRV", kindAdminTo)
	fmt.Println("  Change  2: Domain Users granted AdminTo PRINT-SRV")
	d.createEdge("jdoe", "IT Admins", kindMemberOf)
	fmt.Println("  Change  3: jdoe added to IT Admins")
	d.createEdge("WS-001", "jsmith", kindHasSession)
	fmt.Println("  Change  4: WS-001 has session for jsmith")

	// Changes that actually create attack paths (shortest — 3 hops)
	d.createEdge("jsmith", "Domain Admins", kindMemberOf)
	fmt.Println("  Change  5: jsmith added to Domain Admins group")
	d.createEdge("Contractors", "FILE-SRV-01", kindAdminTo)
	fmt.Println("  Change  6: Contractors group granted AdminTo FILE-SRV-01")
	d.createEdge("FILE-SRV-01", "DA-ADMIN", kindHasSession)
	fmt.Println("  Change  7: FILE-SRV-01 has session for DA-ADMIN")

	// Sneaky longer paths through Backup infrastructure.
	// The 4-hop path to DA-ADMIN will be found (it's the only route to that target).
	// The 4-hop path to DA-SVC will be MISSED — shortest-path already found the 3-hop route.
	d.createNode("Backup Operators", kindGroup)
	d.createNode("Server Admins", kindGroup)
	d.createNode("BACKUP-SRV", kindComputer)
	d.createEdge("jsmith", "Backup Operators", kindMemberOf)
	fmt.Println("  Change  8: jsmith added to Backup Operators")
	d.createEdge("Backup Operators", "Server Admins", kindMemberOf)
	fmt.Println("  Change  9: Backup Operators nested into Server Admins")
	d.createEdge("Server Admins", "BACKUP-SRV", kindAdminTo)
	fmt.Println("  Change 10: Server Admins granted AdminTo BACKUP-SRV")
	d.createEdge("BACKUP-SRV", "DA-ADMIN", kindHasSession)
	fmt.Println("  Change 11: BACKUP-SRV has session for DA-ADMIN")
	d.createEdge("BACKUP-SRV", "DA-SVC", kindHasSession)
	fmt.Println("  Change 12: BACKUP-SRV has session for DA-SVC")

	fmt.Printf("\n  Total: 12 changes. But how many actually matter?\n")

	// ---- Phase 4: Scan again — what changed? ----
	phase("Phase 4: Scanning for attack paths (again)")

	pathsAfter := d.findAttackPaths(attackers, targets)
	fmt.Printf("  Found %d attack path(s) (was %d before):\n", len(pathsAfter), len(pathsBefore))
	for _, p := range pathsAfter {
		d.printPath("    ", p)
	}

	// ---- Phase 5: Time travel ----
	phase("Phase 5: Time travel — querying the graph before the changes")

	fmt.Println("  DAWGS records when every node and edge was created (and deleted).")
	fmt.Println("  That means we can reconstruct the graph as it existed at any prior point")
	fmt.Println("  in time — and run the exact same queries against it.")
	fmt.Println()
	fmt.Println("  The key: there are two ways to open a read transaction:")
	fmt.Println()
	fmt.Println("    db.ReadTransaction(...)             // query the graph as it is NOW")
	fmt.Println("    driver.AsOfReadTransaction(t, ...)  // query the graph as it was at time t")
	fmt.Println()
	fmt.Println("  Inside the callback, the API is identical. Same filters, same pathfinding,")
	fmt.Println("  same cursors. The only difference is which version of the data you see.")

	var currentNodes, currentEdges, historicalNodes, historicalEdges int64

	d.db.ReadTransaction(d.ctx, func(tx graph.Transaction) error {
		currentNodes, _ = tx.Nodes().Count()
		currentEdges, _ = tx.Relationships().Count()
		return nil
	})
	d.driver.AsOfReadTransaction(d.ctx, mark, func(tx graph.Transaction) error {
		historicalNodes, _ = tx.Nodes().Count()
		historicalEdges, _ = tx.Relationships().Count()
		return nil
	})

	fmt.Println()
	fmt.Println("  Let's run the same count query against both views:")
	fmt.Println()
	fmt.Printf("  %-12s  Nodes: %-4d  Edges: %d\n", "NOW:", currentNodes, currentEdges)
	fmt.Printf("  %-12s  Nodes: %-4d  Edges: %d\n", "BEFORE:", historicalNodes, historicalEdges)
	fmt.Println()
	fmt.Printf("  %d nodes and %d edges are new since the last ingestion.\n",
		currentNodes-historicalNodes, currentEdges-historicalEdges)

	fmt.Println()
	fmt.Println("  Now the big question: the attack path scan we just ran — what does it")
	fmt.Println("  return against the historical graph?")

	// Run the exact same attack path query against the historical graph
	historicalPaths := d.findAttackPathsAsOf(attackers, targets, mark)
	fmt.Println()
	fmt.Printf("  Attack paths NOW:    %d\n", len(pathsAfter))
	fmt.Printf("  Attack paths BEFORE: %d\n", len(historicalPaths))
	fmt.Println()
	fmt.Println("  Zero. Every one of these paths was introduced by the changes we just saw.")

	// ---- Phase 6: Forensics — which changes caused which paths? ----
	totalNewEdges := d.countEdgesCreatedAfter(mark)
	phase(fmt.Sprintf("Phase 6: Forensics — %d new edges, which ones caused these %d paths?", totalNewEdges, len(pathsAfter)))

	// Count how many paths each new edge appears on
	edgeHits := make(map[graph.ID]int)
	edgeLabels := make(map[graph.ID]string)

	for _, p := range pathsAfter {
		newEdgeIDs := d.edgesOnPathCreatedAfter(p, mark)
		if len(newEdgeIDs) == 0 {
			continue
		}
		root := d.nodeName(p.Root().ID)
		terminal := d.nodeName(p.Terminal().ID)
		fmt.Printf("\n  Path: %s -> %s\n", root, terminal)
		fmt.Println("  New edges on this path:")
		for _, edge := range p.Edges {
			if !newEdgeIDs[edge.ID] {
				continue
			}
			label := fmt.Sprintf("%s -[%s]-> %s", d.nodeName(edge.StartID), edge.Kind, d.nodeName(edge.EndID))
			fmt.Printf("    + %s\n", label)
			edgeHits[edge.ID]++
			edgeLabels[edge.ID] = label
		}
	}

	fmt.Printf("\n  Result: %d of %d new edges appear on attack paths\n", len(edgeHits), totalNewEdges)

	// ---- Phase 7: Impact ranking ----
	phase("Phase 7: Impact ranking — which single change affected the most paths?")

	type rankedEdge struct {
		id    graph.ID
		label string
		hits  int
	}
	var ranked []rankedEdge
	for id, hits := range edgeHits {
		ranked = append(ranked, rankedEdge{id: id, label: edgeLabels[id], hits: hits})
	}
	for i := range ranked {
		for j := i + 1; j < len(ranked); j++ {
			if ranked[j].hits > ranked[i].hits {
				ranked[i], ranked[j] = ranked[j], ranked[i]
			}
		}
	}
	for i, r := range ranked {
		fmt.Printf("  %d. %s (appears on %d path(s))\n", i+1, r.label, r.hits)
	}

	// ---- Phase 8: Remediation — revert the #1 edge, prove it works ----
	phase("Phase 8: Remediation — what if we revert the #1 impact edge?")

	topEdge := ranked[0]
	fmt.Printf("  The highest-impact change was:\n")
	fmt.Printf("    %s (appears on %d path(s))\n", topEdge.label, topEdge.hits)
	fmt.Println()

	// Find which attackers had paths through this edge
	fmt.Println("  Affected users:")
	type userPaths struct {
		name  string
		paths []graph.Path
	}
	var affectedUsers []userPaths
	for _, attacker := range attackers {
		attackerID := d.nodes[attacker]
		var userPathList []graph.Path
		for _, p := range pathsAfter {
			if p.Root().ID != attackerID {
				continue
			}
			for _, edge := range p.Edges {
				if edge.ID == topEdge.id {
					userPathList = append(userPathList, p)
					break
				}
			}
		}
		if len(userPathList) > 0 {
			affectedUsers = append(affectedUsers, userPaths{attacker, userPathList})
			fmt.Printf("    %s (%d path(s) through this edge)\n", attacker, len(userPathList))
		}
	}

	fmt.Println()
	fmt.Println("  Let's exclude that edge and re-query each affected user.")

	excludeTop := []int64{int64(topEdge.id)}
	for _, u := range affectedUsers {
		fmt.Printf("\n  %s:\n", u.name)
		fmt.Printf("    Before: %d path(s)\n", len(u.paths))

		var survivingCount int
		for _, target := range targets {
			targetID := d.nodes[target]
			err := d.db.ReadTransaction(d.ctx, func(tx graph.Transaction) error {
				return tx.Relationships().Filter(
					query.And(
						query.Equals(query.StartID(), d.nodes[u.name]),
						query.Equals(query.EndID(), targetID),
						query.Not(query.In(query.RelationshipID(), excludeTop)),
					),
				).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
					for range cursor.Chan() {
						survivingCount++
					}
					return cursor.Error()
				})
			})
			if err != nil {
				log.Fatalf("querying surviving paths: %v", err)
			}
		}

		if survivingCount == 0 {
			fmt.Printf("    After:  0 path(s) — clean!\n")
		} else {
			fmt.Printf("    After:  %d path(s) still remain\n", survivingCount)
		}
	}

	fmt.Println()
	fmt.Println("  One revert. Multiple users remediated. That's the power of impact ranking.")

	// ---- Phase 9: The edge case — jsmith's hidden alternate route ----
	phase("Phase 9: But wait — is jsmith actually clean?")

	// jsmith has paths that DON'T go through the top edge
	var jsmithAllPaths []graph.Path
	for _, p := range pathsAfter {
		if p.Root().ID == d.nodes["jsmith"] {
			jsmithAllPaths = append(jsmithAllPaths, p)
		}
	}

	fmt.Printf("  jsmith had %d attack path(s) total:\n", len(jsmithAllPaths))
	for _, p := range jsmithAllPaths {
		d.printPath("    ", p)
	}

	fmt.Println()
	fmt.Println("  We reverted the #1 impact edge. But jsmith also had paths that")
	fmt.Println("  didn't go through it. Let's check ALL of jsmith's paths after the revert.")

	var jsmithSurviving []graph.Path
	for _, target := range targets {
		targetID := d.nodes[target]
		err := d.db.ReadTransaction(d.ctx, func(tx graph.Transaction) error {
			return tx.Relationships().Filter(
				query.And(
					query.Equals(query.StartID(), d.nodes["jsmith"]),
					query.Equals(query.EndID(), targetID),
					query.Not(query.In(query.RelationshipID(), excludeTop)),
				),
			).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
				for path := range cursor.Chan() {
					jsmithSurviving = append(jsmithSurviving, path)
				}
				return cursor.Error()
			})
		})
		if err != nil {
			log.Fatalf("querying jsmith surviving paths: %v", err)
		}
	}

	fmt.Println()
	fmt.Printf("  jsmith still has %d path(s):\n", len(jsmithSurviving))
	for _, p := range jsmithSurviving {
		d.printPath("    SURVIVING", p)
	}

	fmt.Println()
	fmt.Println("  The 3-hop path to DA-SVC was the shortest route. Removing its enabling edge")
	fmt.Println("  revealed a 4-hop path to the same target that was hiding behind it.")
	fmt.Println()
	fmt.Println("  Shortest-path analysis only reports the fastest route to each target.")
	fmt.Println("  Longer alternates are invisible until the shorter path is removed.")
	fmt.Println("  Remediation is iterative: fix, re-query, repeat until clean.")
}

// --- Node/Edge helpers ---

func (d *demo) createNode(name string, kind graph.Kind) {
	var id graph.ID
	err := d.db.WriteTransaction(d.ctx, func(tx graph.Transaction) error {
		node, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": name}), kind)
		if err != nil {
			return err
		}
		id = node.ID
		return nil
	})
	if err != nil {
		log.Fatalf("creating node %s: %v", name, err)
	}
	d.nodes[name] = id
}

func (d *demo) createEdge(startName, endName string, kind graph.Kind) {
	startID := d.nodes[startName]
	endID := d.nodes[endName]

	err := d.db.WriteTransaction(d.ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateRelationshipByIDs(startID, endID, kind, graph.NewProperties())
		return err
	})
	if err != nil {
		log.Fatalf("creating edge %s -> %s: %v", startName, endName, err)
	}
}

func (d *demo) nodeName(id graph.ID) string {
	for name, nid := range d.nodes {
		if nid == id {
			return name
		}
	}
	return fmt.Sprintf("node-%d", id)
}

func (d *demo) mark() time.Time {
	t := time.Now()
	time.Sleep(50 * time.Millisecond)
	return t
}

// --- Temporal query helpers ---

// edgesOnPathCreatedAfter returns the set of edge IDs on the given path that were created
// after the mark time. This queries the database directly — no application-side tracking.
func (d *demo) edgesOnPathCreatedAfter(path graph.Path, mark time.Time) map[graph.ID]bool {
	var edgeIDs []int64
	for _, edge := range path.Edges {
		edgeIDs = append(edgeIDs, int64(edge.ID))
	}

	result := make(map[graph.ID]bool)
	err := d.db.ReadTransaction(d.ctx, func(tx graph.Transaction) error {
		rows := tx.Raw(
			"SELECT id FROM edge WHERE id = ANY(@edge_ids) AND created_at > @mark",
			map[string]any{"edge_ids": edgeIDs, "mark": mark},
		)
		defer rows.Close()

		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return err
			}
			result[graph.ID(id)] = true
		}
		return rows.Error()
	})
	if err != nil {
		log.Fatalf("querying edge created_at: %v", err)
	}
	return result
}

// countEdgesCreatedAfter returns the total number of edges created after the mark time.
func (d *demo) countEdgesCreatedAfter(mark time.Time) int {
	var count int
	err := d.db.ReadTransaction(d.ctx, func(tx graph.Transaction) error {
		rows := tx.Raw(
			"SELECT count(*) FROM edge WHERE created_at > @mark",
			map[string]any{"mark": mark},
		)
		defer rows.Close()

		if rows.Next() {
			return rows.Scan(&count)
		}
		return rows.Error()
	})
	if err != nil {
		log.Fatalf("counting new edges: %v", err)
	}
	return count
}

// --- Pathfinding helpers ---

func (d *demo) findAttackPaths(attackers, targets []string) []graph.Path {
	var paths []graph.Path
	err := d.db.ReadTransaction(d.ctx, func(tx graph.Transaction) error {
		for _, attacker := range attackers {
			for _, target := range targets {
				attackerID := d.nodes[attacker]
				targetID := d.nodes[target]

				err := tx.Relationships().Filter(
					query.And(
						query.Equals(query.StartID(), attackerID),
						query.Equals(query.EndID(), targetID),
					),
				).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
					for path := range cursor.Chan() {
						paths = append(paths, path)
					}
					return cursor.Error()
				})
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("finding attack paths: %v", err)
	}
	return paths
}

func (d *demo) findAttackPathsAsOf(attackers, targets []string, asOf time.Time) []graph.Path {
	var paths []graph.Path
	// Each pathfinding call creates temp tables (forward_front, etc.) with ON COMMIT DROP,
	// so we need a separate AsOfReadTransaction per pair to avoid name collisions.
	for _, attacker := range attackers {
		for _, target := range targets {
			attackerID := d.nodes[attacker]
			targetID := d.nodes[target]

			err := d.driver.AsOfReadTransaction(d.ctx, asOf, func(tx graph.Transaction) error {
				return tx.Relationships().Filter(
					query.And(
						query.Equals(query.StartID(), attackerID),
						query.Equals(query.EndID(), targetID),
					),
				).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
					for path := range cursor.Chan() {
						paths = append(paths, path)
					}
					return cursor.Error()
				})
			})
			if err != nil {
				log.Fatalf("finding historical attack paths: %v", err)
			}
		}
	}
	return paths
}

// --- Display helpers ---

var stdin = bufio.NewReader(os.Stdin)

func phase(title string) {
	fmt.Printf("\n\033[2m[Enter to continue]\033[0m ")
	stdin.ReadBytes('\n')
	fmt.Printf("%s\n%s\n", title, strings.Repeat("=", len(title)))
}

func (d *demo) printPath(label string, path graph.Path) {
	// Build adjacency from edges, then walk from root to terminal for ordered display.
	type hop struct {
		kind   graph.Kind
		nextID graph.ID
	}

	// Map: nodeID -> outgoing hops (treating edges as undirected for traversal)
	adj := make(map[graph.ID][]hop)
	for _, edge := range path.Edges {
		adj[edge.StartID] = append(adj[edge.StartID], hop{edge.Kind, edge.EndID})
		adj[edge.EndID] = append(adj[edge.EndID], hop{edge.Kind, edge.StartID})
	}

	rootID := path.Root().ID
	var parts []string
	parts = append(parts, fmt.Sprintf("(%s)", d.nodeName(rootID)))

	visited := map[graph.ID]bool{rootID: true}
	current := rootID
	for i := 0; i < len(path.Edges); i++ {
		for _, h := range adj[current] {
			if !visited[h.nextID] {
				visited[h.nextID] = true
				parts = append(parts, fmt.Sprintf("-[%s]-> (%s)", h.kind, d.nodeName(h.nextID)))
				current = h.nextID
				break
			}
		}
	}

	fmt.Printf("%s%s\n", label, strings.Join(parts, " "))
}

func (d *demo) printTopology() {
	fmt.Println("\n  Nodes:")
	err := d.db.ReadTransaction(d.ctx, func(tx graph.Transaction) error {
		for _, kind := range []graph.Kind{kindUser, kindGroup, kindComputer, kindDomainAdmin} {
			var names []string
			err := tx.Nodes().Filter(query.Kind(query.Node(), kind)).Fetch(func(cursor graph.Cursor[*graph.Node]) error {
				for node := range cursor.Chan() {
					name, _ := node.Properties.Get("name").String()
					names = append(names, name)
				}
				return cursor.Error()
			})
			if err != nil {
				return err
			}
			if len(names) > 0 {
				fmt.Printf("    %-12s %s\n", kind, strings.Join(names, ", "))
			}
		}

		fmt.Println("\n  Edges:")
		return tx.Relationships().Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for rel := range cursor.Chan() {
				startName := d.nodeName(rel.StartID)
				endName := d.nodeName(rel.EndID)
				fmt.Printf("    %s -[%s]-> %s\n", startName, rel.Kind, endName)
			}
			return cursor.Error()
		})
	})
	if err != nil {
		log.Fatalf("printing topology: %v", err)
	}
}

// --- Cleanup ---

func (d *demo) resetData() {
	d.db.WriteTransaction(d.ctx, func(tx graph.Transaction) error {
		tx.Relationships().Delete()
		tx.Nodes().Delete()
		return nil
	})
	d.db.Run(d.ctx, "DELETE FROM node_deletion_log", nil)
	d.db.Run(d.ctx, "DELETE FROM edge_deletion_log", nil)
}

func (d *demo) cleanup() {
	d.resetData()
	d.db.Close(d.ctx)
}
