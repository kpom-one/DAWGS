package drivers_test

import (
	"context"
	"log"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

// newPGFactory returns a driverFactory backed by a single shared PG connection pool.
// The pool uses pg.NewPool which registers composite types required for node/edge marshalling.
func newPGFactory(connStr string) driverFactory {
	ctx := context.Background()

	pool, err := pg.NewPool(connStr)
	if err != nil {
		log.Fatalf("failed to create pg pool: %v", err)
	}

	driver := pg.NewDriver(size.Gibibyte, pool)

	schema := graph.Schema{
		Graphs: []graph.Graph{benchGraph},
		DefaultGraph: graph.Graph{
			Name: benchGraph.Name,
		},
	}

	if err := driver.AssertSchema(ctx, schema); err != nil {
		log.Fatalf("failed to assert pg schema: %v", err)
	}

	return driverFactory{
		name: "pg",
		newFresh: func() graph.Database {
			return driver
		},
		cleanup: func() {},
	}
}

// TestMain handles PG pool teardown after all benchmarks complete.
func TestMain(m *testing.M) {
	m.Run()
}
