package sonic

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"sync/atomic"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

// Interface compliance assertions.
var _ graph.Database = (*Database)(nil)

const DriverName = "sonic"

func init() {
	dawgs.Register(DriverName, func(ctx context.Context, cfg dawgs.Config) (graph.Database, error) {
		db := NewDatabase()

		if cfg.ConnectionString != "" {
			dataPath, err := parseConnectionString(cfg.ConnectionString)
			if err != nil {
				return nil, err
			}

			db.dataPath = dataPath

			if err := db.loadFromDisk(ctx); err != nil {
				return nil, fmt.Errorf("sonic: failed to load data from %s: %w", db.dataPath, err)
			}
		}

		return db, nil
	})
}

// parseConnectionString parses a sonic:// connection string and returns the file path.
// Examples:
//
//	sonic://data/graph.json       -> data/graph.json  (relative)
//	sonic:///var/data/graph.json  -> /var/data/graph.json (absolute)
func parseConnectionString(connStr string) (string, error) {
	parsed, err := url.Parse(connStr)
	if err != nil {
		return "", fmt.Errorf("sonic: invalid connection string: %w", err)
	}

	if parsed.Scheme != DriverName {
		return "", fmt.Errorf("sonic: connection string must use the %q scheme (e.g. sonic://path/to/graph.json)", DriverName)
	}

	// For sonic://relative/path, Host+Path gives us "relative/path".
	// For sonic:///absolute/path, Host is empty and Path is "/absolute/path".
	path := parsed.Host + parsed.Path
	if path == "" {
		return "", fmt.Errorf("sonic: connection string must include a file path (e.g. sonic://data/graph.json)")
	}

	return path, nil
}

// Database is an in-memory graph database.
type Database struct {
	mu     sync.RWMutex
	nextID atomic.Uint64
	nodes  map[graph.ID]*graph.Node
	edges  map[graph.ID]*graph.Relationship

	// Adjacency indexes
	outEdges map[graph.ID][]graph.ID // nodeID -> edgeIDs where node is start
	inEdges  map[graph.ID][]graph.ID // nodeID -> edgeIDs where node is end

	schema       graph.Schema
	defaultGraph graph.Graph
	kinds        graph.Kinds

	queryMemoryLimit size.Size
	batchWriteSize   int
	writeFlushSize   int

	// dataPath is the opengraph JSON file used for persistence.
	// If empty, the database is purely in-memory with no persistence.
	dataPath string
	dirty    bool
}

func NewDatabase() *Database {
	return &Database{
		nodes:    make(map[graph.ID]*graph.Node),
		edges:    make(map[graph.ID]*graph.Relationship),
		outEdges: make(map[graph.ID][]graph.ID),
		inEdges:  make(map[graph.ID][]graph.ID),
	}
}

func (db *Database) newID() graph.ID {
	return graph.ID(db.nextID.Add(1))
}

func (db *Database) SetWriteFlushSize(interval int) {
	db.writeFlushSize = interval
}

func (db *Database) SetBatchWriteSize(interval int) {
	db.batchWriteSize = interval
}

func (db *Database) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	return txDelegate(&transaction{db: db, ctx: ctx})
}

func (db *Database) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	err := txDelegate(&transaction{db: db, ctx: ctx})
	if err == nil {
		db.dirty = true
	}
	return err
}

func (db *Database) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate, options ...graph.BatchOption) error {
	err := batchDelegate(&batch{db: db, ctx: ctx})
	if err == nil {
		db.dirty = true
	}
	return err
}

func (db *Database) AssertSchema(ctx context.Context, dbSchema graph.Schema) error {
	db.schema = dbSchema
	db.defaultGraph = dbSchema.DefaultGraph
	return nil
}

func (db *Database) SetDefaultGraph(ctx context.Context, graphSchema graph.Graph) error {
	db.defaultGraph = graphSchema
	return nil
}

func (db *Database) Run(ctx context.Context, query string, parameters map[string]any) error {
	// No-op for in-memory driver — raw queries are not supported.
	return nil
}

func (db *Database) Close(ctx context.Context) error {
	if err := db.saveToDisk(ctx); err != nil {
		log.Printf("sonic: failed to save data to %s: %v", db.dataPath, err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.nodes = make(map[graph.ID]*graph.Node)
	db.edges = make(map[graph.ID]*graph.Relationship)
	db.outEdges = make(map[graph.ID][]graph.ID)
	db.inEdges = make(map[graph.ID][]graph.ID)
	return nil
}

// loadFromDisk loads the opengraph JSON file at db.dataPath into memory.
// If the file does not exist, this is a no-op (fresh database).
func (db *Database) loadFromDisk(ctx context.Context) error {
	f, err := os.Open(db.dataPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := opengraph.Load(ctx, db, f); err != nil {
		return err
	}

	db.dirty = false
	return nil
}

// saveToDisk writes the current graph state to the opengraph JSON file at db.dataPath.
// No-op if dataPath is empty or the database hasn't been modified since the last save/load.
//
// TODO: Consider exposing a public Snapshot() method for callers that want to
// flush to disk on demand (e.g. after large ingests) without closing the database.
func (db *Database) saveToDisk(ctx context.Context) error {
	if db.dataPath == "" || !db.dirty {
		return nil
	}

	f, err := os.Create(db.dataPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := opengraph.Export(ctx, db, f); err != nil {
		return err
	}

	db.dirty = false
	return nil
}

func (db *Database) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	return db.kinds, nil
}

func (db *Database) RefreshKinds(ctx context.Context) error {
	return nil
}
