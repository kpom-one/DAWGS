package opengraph

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
)

var defaultKinds = struct {
	nodes []string
	edges []string
}{
	nodes: []string{"User", "Group", "Computer", "DomainAdmin"},
	edges: []string{"MemberOf", "AdminTo", "HasSession"},
}

// Generate writes a random OpenGraph JSON document with the given number of nodes
// and edges to w. Edges connect random pairs of nodes. The output is deterministic
// for a given seed.
func Generate(w io.Writer, nodes, edges int, seed int64) error {
	rng := rand.New(rand.NewSource(seed))

	doc := Document{
		Graph: Graph{
			Nodes: make([]Node, nodes),
			Edges: make([]Edge, edges),
		},
	}

	for i := range doc.Graph.Nodes {
		id := fmt.Sprintf("node-%d", i)
		doc.Graph.Nodes[i] = Node{
			ID:    id,
			Kinds: []string{defaultKinds.nodes[i%len(defaultKinds.nodes)]},
			Properties: map[string]any{
				"name": id,
			},
		}
	}

	for i := range doc.Graph.Edges {
		s := rng.Intn(nodes)
		d := rng.Intn(nodes)
		if s == d {
			d = (d + 1) % nodes
		}

		doc.Graph.Edges[i] = Edge{
			Start: NodeRef{Value: fmt.Sprintf("node-%d", s)},
			End:   NodeRef{Value: fmt.Sprintf("node-%d", d)},
			Kind:  defaultKinds.edges[i%len(defaultKinds.edges)],
		}
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(doc)
}
