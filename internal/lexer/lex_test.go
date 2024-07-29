package lexer

import "testing"

func TestLexer(t *testing.T) {
	testStatement := `CREATE (i:Identity:Person {id: "987654"})-[:POSTED]->(p:Post {id: "123456", uri: "ipfs://xyz"})`

	l := lex("test", testStatement)
	l.run()

}
