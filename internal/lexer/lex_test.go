package lexer

import "testing"

func TestLexer(t *testing.T) {
	testStatement := `MERGE (i:Identity:Person {id: '987654'})-[:POSTED]->(p:Post {id: "123456", uri: 'ipfs://xyz', count: 1, test: 'hello\tworld'})`

	l := lex("test", testStatement)
	l.run()

}
