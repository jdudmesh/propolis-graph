package executor

import (
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

	store, err := datastore.NewInternalState("./migrations", []string{}, []string{})
	assert.NoError(err)

	testStatement := `MERGE (i:Identity:Person {id: '987654'})-[:POSTED]->(p:Post {id: "123456", uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

	l := ast.Lex("test", testStatement)
	l.Run()

	p := ast.Parse(l)
	err = p.Run()
	assert.NoError(err)

	e, err := New(p.Entities(), store, logger)
	assert.NotNil(e)
	assert.NoError(err)

	err = e.Execute()
	assert.NoError(err)
}
