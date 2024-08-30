package graph

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

import (
	"fmt"
	"testing"
	"time"

	"github.com/jdudmesh/propolis/internal/ast"
	"github.com/stretchr/testify/assert"
)

func TestExecutorCRUD(t *testing.T) {
	assert := assert.New(t)

	testStmt := `MERGE (i:Identity:Person {name: 'john'})-[:posted{ipAddress:'127.0.0.1'}]->(p:Post {uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

	p, err := ast.Parse(testStmt)
	assert.NoError(err)

	ids := []string{}
	t.Run("create", func(t *testing.T) {
		e, err := New(config)
		assert.NoError(err)
		assert.NotNil(e)

		action := Action{
			ID:       "12345.67890",
			Identity: "11111111",
			Command:  p.Command(),
		}
		res, err := e.Execute(action)
		assert.NoError(err)
		assert.NotNil(p.Command())
		assert.IsType(&Relation{}, res)
		ids = append(ids, res.(*Relation).ID)
		ids = append(ids, res.(*Relation).LeftNodeID)
		ids = append(ids, res.(*Relation).RightNodeID)
	})

	t.Run("update - with perms", func(t *testing.T) {
		// make sure previous insert found
		e, err := New(config)
		assert.NoError(err)
		assert.NotNil(e)

		action := Action{
			ID:       "12345.67890",
			Identity: "11111111",
			Command:  p.Command(),
		}
		res, err := e.Execute(action)
		assert.NoError(err)
		assert.NotNil(res)
		assert.IsType(&Relation{}, res)
		assert.Equal(ids[0], res.(*Relation).ID)
		assert.Equal(ids[1], res.(*Relation).LeftNodeID)
		assert.Equal(ids[2], res.(*Relation).RightNodeID)
	})

	t.Run("update - without perms", func(t *testing.T) {
		// make sure previous insert found
		e, err := New(config)
		assert.NoError(err)
		assert.NotNil(e)

		action := Action{
			ID:       "12345.67890",
			Identity: "22222222",
			Command:  p.Command(),
		}
		res, err := e.Execute(action)
		assert.ErrorIs(err, ErrUnauthorized)
		assert.Nil(res)
	})
}

func TestExecutorSearch(t *testing.T) {
	assert := assert.New(t)

	testStmt1 := `MERGE (i:Identity:Person {name: 'john'})-[:posted{ipAddress:'127.0.0.1'}]->(p:Post {uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

	p, err := ast.Parse(testStmt1)
	assert.NoError(err)

	e, err := New(config)
	assert.NoError(err)
	assert.NotNil(e)

	action := Action{
		ID:      "12345.67890",
		Command: p.Command(),
	}
	_, err = e.Execute(action)
	assert.NoError(err)

	now := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	testStmt2 := fmt.Sprintf("MATCH (i:Identity:Person {name: 'john'})-[r]-(c) SINCE '%s'", now)

	t.Run("find", func(t *testing.T) {
		p, err := ast.Parse(testStmt2)
		assert.NoError(err)
		assert.NotNil(p.Command())

		e, err := New(config)
		assert.NoError(err)
		assert.NotNil(e)

		action2 := Action{
			ID:      "12345.67890",
			Command: p.Command(),
		}
		res, err := e.Execute(action2)
		assert.NoError(err)
		assert.NotNil(res)
	})

}
