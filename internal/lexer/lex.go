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
	numeric      = "0123456789"
	spaces       = " \t\n"
	braces       = "{}"
	colon        = ":"
	quotes       = "\"'"
	escapeChar   = "\\"
)

// itemType identifies the type of lex items.
type itemType int

const (
	itemError itemType = iota // error occurred; value is text of error
	itemEOF
	itemSpace          // run of spaces separating arguments
	itemCommand        // e.g. MERGE DELETE etc
	itemString         // quoted string (includes quotes)
	itemText           // plain text
	itemNumber         // simple number
	itemNodeIdentifier //
	itemNodeLabelStart
	itemNodeLabel //
	itemStartNode
	itemEndNode
	itemRelationDirNeutral
	itemRelationDirLeft
	itemRelationDirRight
	itemRelationStart
	itemRelationEnd
	itemRelationIdentifier
	itemRelationLabelStart
	itemRelationLabel
	itemLeftRelation
	itemRightRelation
	itemLeftAttribs
	itemRightAttribs
	itemAttribSeparator
	itemAttribIdentifier
	itemAttribValue

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

// acceptRun consumes a run of runes from the valid set. the run may be quoted
func (l *lexer) acceptQuotedRun(valid string) {
	n := l.peek()
	if n == '\'' || n == '"' {
		l.lexQuotedRun()
		return
	}

	for strings.ContainsRune(valid, l.next()) {
	}
	l.backup()
}

func (l *lexer) lexQuotedRun() {
	quoteChar := l.next()
	isEscapeSeq := false
	for {
		n := l.next()
		switch {
		case n == quoteChar && !isEscapeSeq:
			return
		case n == '\\':
			isEscapeSeq = true
		default:
			isEscapeSeq = false
		}
	}
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
	if int(l.pos) >= len(l.input) {
		return lexEOF
	}

	l.acceptRun(spaces)

	if l.pos >= len(l.input) {
		return lexEOF
	}

	n := l.peek()
	switch {
	case strings.ContainsRune(alphanumeric, n):
		l.acceptRun(alphanumeric)
		i := l.thisItem(itemKeyword)
		if t, ok := keywords[strings.ToLower(i.val)]; ok {
			i.typ = t
			l.emitItem(i)
			return lexClause
		}
	case n == '(':
		return lexNodeStart
	case n == ')':
		return lexNodeEnd
	case n == ':':
		return lexNodeLabelStart
	case n == '-':
		return lexRelationDirNeutral
	case n == '<':
		return lexRelationDirLeftStart
	case n == '[':
		return lexRelationStart
	case n == ']':
		return lexRelationEnd
	}

	l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
	return nil
}

func lexNodeStart(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	r := l.next()
	if r != '(' {
		l.errorf("syntax error, expected '(': %s", l.input[l.start:l.pos])
		return nil
	}

	i := l.thisItem(itemStartNode)
	l.emitItem(i)

	return lexNodeInner
}

func lexNodeInner(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	n := l.peek()
	switch {
	case strings.ContainsRune(alphanumeric, n):
		return lexNodeIdentifier
	case n == ':':
		return lexNodeLabelStart
	case n == '{':
		return lexAttribStart
	case n == '}':
		return lexAttribEnd
	case n == ')':
		return lexNodeEnd
	}
	return nil
}

func lexNodeIdentifier(l *lexer) stateFn {
	l.acceptRun(alphanumeric)

	i := l.thisItem(itemNodeIdentifier)
	l.emitItem(i)

	return lexNodeInner
}

func lexNodeLabelStart(l *lexer) stateFn {
	l.next()
	i := l.thisItem(itemNodeLabelStart)
	l.emitItem(i)
	return lexNodeLabel
}

func lexNodeLabel(l *lexer) stateFn {
	l.acceptRun(alphanumeric)
	i := l.thisItem(itemNodeLabel)
	l.emitItem(i)

	return lexNodeInner
}

func lexAttribStart(l *lexer) stateFn {
	r := l.next()
	if r != '{' {
		l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
	}

	i := l.thisItem(itemLeftAttribs)
	l.emitItem(i)
	return lexNodeAttrib
}

func lexAttribEnd(l *lexer) stateFn {
	r := l.next()
	if r != '}' {
		l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
	}

	i := l.thisItem(itemRightAttribs)
	l.emitItem(i)
	return lexNodeInner
}

func lexNodeAttrib(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	n := l.peek()
	switch {
	case strings.ContainsRune(alphanumeric, n):
		return lexNodeAttribIdentifier
	case n == ':':
		return lexNodeAttribSeparator
	case n == ',':
		l.next()
		l.ignore()
		return lexNodeAttrib
	case n == '}':
		return lexAttribEnd
	}

	return nil
}

func lexNodeAttribIdentifier(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	l.acceptRun(alphanumeric)
	i := l.thisItem(itemAttribIdentifier)
	l.emitItem(i)

	return lexNodeAttribSeparator
}

func lexNodeAttribSeparator(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	r := l.next()
	if r != ':' {
		l.errorf("syntax error: %s", l.input[l.start:l.pos])
		return nil
	}

	i := l.thisItem(itemAttribSeparator)
	l.emitItem(i)

	return lexNodeAttribValue
}

func lexNodeAttribValue(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	l.acceptQuotedRun(numeric)
	i := l.thisItem(itemAttribValue)
	l.emitItem(i)

	return lexNodeAttrib
}

func lexNodeEnd(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	r := l.next()
	if r != ')' {
		l.errorf("syntax error: %s", l.input[l.start:l.pos])
	}

	i := l.thisItem(itemEndNode)
	l.emitItem(i)

	return lexClause
}

func lexRelationDirNeutral(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	r1 := l.next()
	if r1 != '-' {
		l.errorf("syntax error: %s", l.input[l.start:l.pos])
	}

	r2 := l.next()
	if r2 != '>' {
		l.backup()
		i := l.thisItem(itemRelationDirNeutral)
		l.emitItem(i)
	} else {
		i := l.thisItem(itemRelationDirRight)
		l.emitItem(i)
	}

	return lexClause
}

func lexRelationDirLeftStart(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	r1 := l.next()
	if r1 != '-' {
		l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
	}

	i := l.thisItem(itemRelationDirLeft)
	l.emitItem(i)

	return lexClause
}

func lexRelationStart(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	r1 := l.next()
	if r1 != '[' {
		l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
	}

	i := l.thisItem(itemRelationStart)
	l.emitItem(i)

	return lexRelationInner
}

func lexRelationEnd(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	r1 := l.next()
	if r1 != ']' {
		l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
	}

	i := l.thisItem(itemRelationEnd)
	l.emitItem(i)

	return lexClause
}

func lexRelationInner(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	n := l.peek()
	switch {
	case strings.ContainsRune(alphanumeric, n):
		return lexRelationIdentifier
	case n == ':':
		return lexRelationLabelStart
	case n == '{':
		return lexRelationAttribStart
	case n == '}':
		return lexRelationAttribEnd
	case n == ']':
		return lexRelationEnd
	}

	return nil
}

func lexRelationIdentifier(l *lexer) stateFn {
	l.acceptRun(alphanumeric)

	i := l.thisItem(itemRelationIdentifier)
	l.emitItem(i)

	return lexNodeInner
}

func lexRelationLabelStart(l *lexer) stateFn {
	r := l.next()
	if r != ':' {
		l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
	}
	i := l.thisItem(itemRelationLabelStart)
	l.emitItem(i)
	return lexRelationLabel
}

func lexRelationLabel(l *lexer) stateFn {
	l.acceptRun(alphanumeric)
	i := l.thisItem(itemRelationLabel)
	l.emitItem(i)

	return lexRelationInner
}

func lexRelationAttribStart(l *lexer) stateFn {
	r := l.next()
	if r != '{' {
		l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
	}

	i := l.thisItem(itemLeftAttribs)
	l.emitItem(i)
	return lexRelationAttrib
}

func lexRelationAttribEnd(l *lexer) stateFn {
	r := l.next()
	if r != '}' {
		l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
	}

	i := l.thisItem(itemRightAttribs)
	l.emitItem(i)
	return lexRelationInner
}

func lexRelationAttrib(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	n := l.peek()
	switch {
	case strings.ContainsRune(alphanumeric, n):
		return lexRelationAttribIdentifier
	case n == ':':
		return lexRelationAttribSeparator
	case n == ',':
		l.next()
		l.ignore()
		return lexRelationAttrib
	case n == '}':
		return lexAttribEnd
	}

	return nil
}

func lexRelationAttribIdentifier(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	l.acceptRun(alphanumeric)
	i := l.thisItem(itemAttribIdentifier)
	l.emitItem(i)

	return lexRelationAttribSeparator
}

func lexRelationAttribSeparator(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	r := l.next()
	if r != ':' {
		l.errorf("syntax error: %s (%d)", l.input[l.start:l.pos], l.pos)
		return nil
	}

	i := l.thisItem(itemAttribSeparator)
	l.emitItem(i)

	return lexRelationAttribValue
}

func lexRelationAttribValue(l *lexer) stateFn {
	l.acceptRun(spaces)
	l.ignore()

	l.acceptRun(alphanumeric + quotes + escapeChar)
	i := l.thisItem(itemAttribValue)
	l.emitItem(i)

	return lexRelationAttrib
}
