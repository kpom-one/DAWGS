package opengraph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/specterops/dawgs/graph"
)

// Load decodes an OpenGraph JSON document from r and writes all nodes and edges into db.
// It returns a map from the string node IDs in the document to their database-assigned IDs.
func Load(ctx context.Context, db graph.Database, r io.Reader) (map[string]graph.ID, error) {
	var doc Document
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, fmt.Errorf("opengraph: decoding JSON: %w", err)
	}

	idMap := make(map[string]graph.ID, len(doc.Graph.Nodes))

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, n := range doc.Graph.Nodes {
			kinds := make([]graph.Kind, 0, len(n.Kinds)+1)

			if doc.Metadata != nil && doc.Metadata.SourceKind != "" {
				kinds = append(kinds, graph.StringKind(doc.Metadata.SourceKind))
			}

			for _, k := range n.Kinds {
				kinds = append(kinds, graph.StringKind(k))
			}

			var props *graph.Properties
			if len(n.Properties) > 0 {
				props = graph.AsProperties(n.Properties)
			} else {
				props = graph.NewProperties()
			}

			node, err := tx.CreateNode(props, kinds...)
			if err != nil {
				return fmt.Errorf("creating node %q: %w", n.ID, err)
			}

			idMap[n.ID] = node.ID
		}

		for _, e := range doc.Graph.Edges {
			startID, ok := idMap[e.Start.Value]
			if !ok {
				return fmt.Errorf("edge references unknown start node %q", e.Start.Value)
			}

			endID, ok := idMap[e.End.Value]
			if !ok {
				return fmt.Errorf("edge references unknown end node %q", e.End.Value)
			}

			var props *graph.Properties
			if len(e.Properties) > 0 {
				props = graph.AsProperties(e.Properties)
			} else {
				props = graph.NewProperties()
			}

			if _, err := tx.CreateRelationshipByIDs(startID, endID, graph.StringKind(e.Kind), props); err != nil {
				return fmt.Errorf("creating edge %s -[%s]-> %s: %w", e.Start.Value, e.Kind, e.End.Value, err)
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return idMap, nil
}
