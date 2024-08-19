/*
Copyright © 2024 John Dudmesh <john@dudmesh.co.uk>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/
package executor

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jdudmesh/propolis/internal/ast"
	"github.com/jdudmesh/propolis/internal/datastore"
	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T) (store, *slog.Logger) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))

	cur := os.Getenv("WORKSPACE_DIR")
	//dbConn := "file::memory:?cache=shared"
	dbConn := fmt.Sprintf("file:%s/data/propolis.db?mode=rwc&_secure_delete=true", cur)
	store, err := datastore.NewInternalState(dbConn, cur+"/migrations", []string{}, []string{})
	assert.NoError(t, err)
	if store == nil {
		t.Fatal("no store")
	}
	return store, logger
}

func TestExecutorCRUD(t *testing.T) {
	assert := assert.New(t)
	store, logger := setup(t)

	testStmt := `MERGE (i:Identity:Person {name: 'john'})-[:posted{ipAddress:'127.0.0.1'}]->(p:Post {uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

	p, err := ast.Parse(testStmt)
	assert.NoError(err)

	ids := []string{}
	t.Run("create", func(t *testing.T) {
		e, err := New(p.Command(), store, logger)
		assert.NotNil(e)
		assert.NoError(err)

		res, err := e.Execute()
		assert.NoError(err)
		assert.NotNil(p.Command())
		assert.IsType(&Relation{}, res)
		ids = append(ids, res.(*Relation).ID)
		ids = append(ids, res.(*Relation).LeftNodeID)
		ids = append(ids, res.(*Relation).RightNodeID)
	})

	t.Run("update", func(t *testing.T) {
		// make sure previous insert found
		e, err := New(p.Command(), store, logger)
		assert.NotNil(e)
		assert.NoError(err)

		res, err := e.Execute()
		assert.NoError(err)
		assert.NotNil(res)
		assert.IsType(&Relation{}, res)
		assert.Equal(ids[0], res.(*Relation).ID)
		assert.Equal(ids[1], res.(*Relation).LeftNodeID)
		assert.Equal(ids[2], res.(*Relation).RightNodeID)
	})

}

func TestExecutorQuery(t *testing.T) {
	assert := assert.New(t)
	store, logger := setup(t)

	testStmt1 := `MERGE (i:Identity:Person {name: 'john'})-[:posted{ipAddress:'127.0.0.1'}]->(p:Post {uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

	p, err := ast.Parse(testStmt1)
	assert.NoError(err)

	e, err := New(p.Command(), store, logger)
	assert.NotNil(e)
	assert.NoError(err)

	_, err = e.Execute()
	assert.NoError(err)

	now := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	testStmt2 := fmt.Sprintf("MATCH (i:Identity:Person {name: 'john'})-[r]-(c) SINCE '%s'", now)

	t.Run("find", func(t *testing.T) {
		p, err := ast.Parse(testStmt2)
		assert.NoError(err)
		assert.NotNil(p.Command())

		e, err := New(p.Command(), store, logger)
		assert.NotNil(e)
		assert.NoError(err)

		res, err := e.Execute()
		assert.NoError(err)
		assert.NotNil(res)

	})

}
