package opengraph

// Document is the top-level OpenGraph JSON structure.
type Document struct {
	Metadata *Metadata `json:"metadata,omitempty"`
	Graph    Graph     `json:"graph"`
}

// Metadata contains optional document-level information.
type Metadata struct {
	SourceKind string `json:"source_kind,omitempty"`
}

// Graph holds the nodes and edges of an OpenGraph document.
type Graph struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

// Node represents a graph node with an ID, one or more kinds, and optional properties.
type Node struct {
	ID         string         `json:"id"`
	Kinds      []string       `json:"kinds"`
	Properties map[string]any `json:"properties,omitempty"`
}

// Edge represents a directed relationship between two nodes.
type Edge struct {
	Start      NodeRef        `json:"start"`
	End        NodeRef        `json:"end"`
	Kind       string         `json:"kind"`
	Properties map[string]any `json:"properties,omitempty"`
}

// NodeRef identifies a node endpoint for an edge.
type NodeRef struct {
	MatchBy string `json:"match_by,omitempty"`
	Value   string `json:"value"`
	Kind    string `json:"kind,omitempty"`
}
