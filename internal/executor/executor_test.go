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
	dbConn := "file::memory:?cache=shared"
	//dbConn := fmt.Sprintf("file:%s/data/propolis.db?mode=rwc&_secure_delete=true", cur)
	store, err := datastore.NewInternalState(dbConn, cur+"/migrations", []string{}, []string{})
	assert.NoError(err)
	if store == nil {
		t.Fatal("no store")
	}

	t.Run("create and update", func(t *testing.T) {
		testStmt := `MERGE (i:Identity:Person {name: 'john'})-[:posted{ipAddress:'127.0.0.1'}]->(p:Post {uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

		l := ast.Lex("test", testStmt)
		l.Run()

		p := ast.Parse(l)
		err = p.Run()
		assert.NoError(err)

		ents := p.Entities()
		ids := make([]string, 0, len(ents))
		for _, ent := range ents {
			e, err := New(ent, store, logger)
			assert.NotNil(e)
			assert.NoError(err)

			res, err := e.Execute()
			assert.NoError(err)
			assert.NotNil(ent)
			assert.IsType(&Relation{}, res)
			ids = append(ids, res.(*Relation).ID)
		}

		// make sure previous insert found
		for i, ent := range ents {
			e, err := New(ent, store, logger)
			assert.NotNil(e)
			assert.NoError(err)

			res, err := e.Execute()
			assert.NoError(err)
			assert.NotNil(res)
			assert.IsType(&Relation{}, res)
			assert.Equal(ids[i], res.(*Relation).ID)
		}
	})

	t.Run("find", func(t *testing.T) {
		testStmt := `MATCH (i:Identity:Person {name: 'john'})`
		l := ast.Lex("test", testStmt)
		l.Run()

		p := ast.Parse(l)
		err = p.Run()
		assert.NoError(err)
		assert.NotZero(len(p.Entities()))

		e, err := New(p.Entities()[0], store, logger)
		assert.NotNil(e)
		assert.NoError(err)

		res, err := e.Execute()
		assert.NoError(err)
		assert.NotNil(res)

	})

}
