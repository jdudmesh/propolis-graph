package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer(t *testing.T) {
	assert := assert.New(t)

	testStatement := `MERGE (i:Identity:Person {id: '987654'})-[:POSTED]->(p:Post {id: "123456", uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

	l := Lex("test", testStatement)
	l.Run()

	p := Parse(l)
	err := p.Run()
	assert.NoError(err)
}
