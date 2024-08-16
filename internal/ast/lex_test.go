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
package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer(t *testing.T) {
	assert := assert.New(t)

	testStatement := `MERGE (i:Identity:Person {id: '987654'})-[:POSTED]->(p:Post {id: "123456", uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

	p, err := Parse(testStatement)
	assert.NoError(err)
	assert.NotNil(p)
}
