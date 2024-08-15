package executor

import (
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/jdudmesh/propolis/internal/ast"
	"github.com/jdudmesh/propolis/internal/datastore"
	"github.com/stretchr/testify/assert"
)

func TestExecutor(t *testing.T) {
	assert := assert.New(t)

	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))

	cur := os.Getenv("WORKSPACE_DIR")
	//dbConn := "file::memory:?cache=shared"
	dbConn := fmt.Sprintf("file:%s/data/propolis.db?mode=rwc&_secure_delete=true", cur)
	store, err := datastore.NewInternalState(dbConn, cur+"/migrations", []string{}, []string{})
	assert.NoError(err)
	if store == nil {
		t.Fatal("no store")
	}

	testStatement := `MERGE (i:Identity:Person {name: 'john'})-[:posted{ipAddress:'127.0.0.1'}]->(p:Post {uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

	l := ast.Lex("test", testStatement)
	l.Run()

	p := ast.Parse(l)
	err = p.Run()
	assert.NoError(err)

	e, err := New(p.Entities(), store, logger)
	assert.NotNil(e)
	assert.NoError(err)

	ent, err := e.Execute()
	assert.NoError(err)
	assert.NotNil(ent)

}
