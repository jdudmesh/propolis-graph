package lexer

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

/*
https://www.youtube.com/watch?v=HxaD_trXwRE
Based heavily on the lexer in text/template
language spec: https://opencypher.org/ https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf
*/

const (
	alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	spaces       = " \t\n"
)

// itemType identifies the type of lex items.
type itemType int

const (
	itemError itemType = iota // error occurred; value is text of error
	itemEOF
	itemSpace      // run of spaces separating arguments
	itemCommand    // e.g. MERGE DELETE etc
	itemString     // quoted string (includes quotes)
	itemText       // plain text
	itemNumber     // simple number
	itemIdentifier //
	itemLabel      //
	itemLeftNode
	itemRightNode
	itemLeftRelation
	itemRightRelation
	itemLeftAttribs
	itemRightAttribs

	itemKeyword // keywords follow
	itemMatch
	itemMerge
	itemCreate
	itemDelete
	itemSet
	itemSubscribe
	itemUnsubscribe
)

// item represents a token or text string returned from the scanner.
type item struct {
	typ itemType // The type of this item.
	pos int      // The starting position, in bytes, of this item in the input string.
	val string   // The value of this item.
}

var keywords = map[string]itemType{
	"match":       itemMatch,
	"merge":       itemMerge,
	"create":      itemCreate,
	"delete":      itemDelete,
	"set":         itemSet,
	"subscribe":   itemSubscribe,
	"unsubscribe": itemUnsubscribe,
}

const eof = -1

func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	case i.typ > itemKeyword:
		return fmt.Sprintf("<%s>", i.val)
	case len(i.val) > 10:
		return fmt.Sprintf("%.10q...", i.val)
	}
	return fmt.Sprintf("%q", i.val)
}

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*lexer) stateFn

const (
	spaceChars = " \t\r\n" // These are the space characters defined by Go itself.
)

// lexer holds the state of the scanner.
type lexer struct {
	name  string // the name of the input; used only for error reports
	input string // the string being scanned
	pos   int    // current position in the input
	start int    // start position of this item
	items []item // item to return to parser
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	if int(l.pos) >= len(l.input) {
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += w
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup steps back one rune.
func (l *lexer) backup() {
	_, w := utf8.DecodeLastRuneInString(l.input[:l.pos])
	l.pos -= w
}

// thisItem returns the item at the current input point with the specified type
// and advances the input.
func (l *lexer) thisItem(t itemType) item {
	i := item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
	return i
}

// emit passes the trailing text as an item back to the parser.
func (l *lexer) emit(t itemType) stateFn {
	return l.emitItem(l.thisItem(t))
}

// emitItem passes the specified item to the parser.
func (l *lexer) emitItem(i item) stateFn {
	l.items = append(l.items, i)
	return nil
}

// ignore skips over the pending input before this point.
// It tracks newlines in the ignored text, so use it only
// for text that is skipped without calling l.next.
func (l *lexer) ignore() {
	l.start = l.pos
}

// accept consumes the next rune if it's from the valid set.
func (l *lexer) accept(valid string) bool {
	if strings.ContainsRune(valid, l.next()) {
		return true
	}
	l.backup()
	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *lexer) acceptRun(valid string) {
	for strings.ContainsRune(valid, l.next()) {
	}
	l.backup()
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *lexer) errorf(format string, args ...any) stateFn {
	l.emitItem(item{itemError, l.start, fmt.Sprintf(format, args...)})
	l.start = 0
	l.pos = 0
	l.input = l.input[:0]
	return nil
}

// nextItem returns the next item from the input.
// Called by the parser, not in the lexing goroutine.
func (l *lexer) run() {
	state := lexClause
	for {
		state = state(l)
		if state == nil {
			return
		}
	}
}

// lex creates a new scanner for the input string.
func lex(name, input string) *lexer {
	l := &lexer{
		name:  name,
		input: input,
		items: make([]item, 0),
	}
	return l
}

func lexEOF(l *lexer) stateFn {
	return nil
}

func lexClause(l *lexer) stateFn {
	l.acceptRun(spaces)

	if l.pos >= len(l.input) {
		return lexEOF
	}

	l.acceptRun(alphanumeric)

	i := item{itemKeyword, l.start, l.input[l.start:l.pos]}

	if t, ok := keywords[strings.ToLower(i.val)]; ok {
		i.typ = t
		l.emitItem(i)
		l.acceptRun(spaces)
		return lexNodeStart
	}

	l.errorf("unknown clause: %s", l.input[l.start:l.pos])
	return nil
}

func lexNodeStart(l *lexer) stateFn {
	if l.pos >= len(l.input) {
		return lexEOF
	}

	r := l.next()
	if r != '(' {
		l.errorf("expected start of node definitions: %s", l.input[l.start:l.pos])
	}

	return lexNodeInner
}

func lexNodeInner(l *lexer) stateFn {
	l.acceptRun(spaces)
	return nil
}

func lexNodeEnd(l *lexer) stateFn {
	if l.pos >= len(l.input) {
		return lexEOF
	}

	r := l.next()
	if r != ')' {
		l.errorf("expected emd of node definitions: %s", l.input[l.start:l.pos])
	}

	return lexNodeRelationshipStart
}

func lexNodeRelationshipStart(l *lexer) stateFn {
	l.acceptRun(spaces)
	if l.pos >= len(l.input) {
		return lexEOF
	}

	return nil
}
