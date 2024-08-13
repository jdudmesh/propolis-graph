package ast

import (
	"errors"
	"fmt"
)

var (
	ErrUnexpectedEndOfInput = errors.New("unexpected end of input")
)

type ParseableEntity interface {
	Parse(p *parser) error
	Identifier() string
	Labels() []string
	Attributes() map[string]any
}

type MergeCmd struct {
}

type MatchCmd struct {
}

type Entity struct {
	identifier string
	labels     []string
	attributes map[string]any
}

func (e Entity) Identifier() string {
	return e.identifier
}

func (e Entity) Labels() []string {
	return e.labels
}

func (e Entity) Attributes() map[string]any {
	return e.attributes
}

func (e *Entity) parseAttr(p *parser) error {
	pendingVar := ""
	for {
		i := p.pop()
		switch i.typ {
		case itemAttributesEnd:
			p.accept()
			return nil
		case itemAttribSeparator:
			p.accept()
		case itemAttribIdentifier:
			pendingVar = i.val
		case itemAttribValue:
			if pendingVar == "" {
				return fmt.Errorf("unexpected input: %s (%d)", i.val, i.pos)
			}
			e.attributes[pendingVar] = i.val
			pendingVar = ""
		case itemEOF:
			return ErrUnexpectedEndOfInput
		default:
			return fmt.Errorf("unknown token: %s (%d)", i.val, i.pos)
		}
	}
}

type Node struct {
	Entity
}

type RelationDir int

const (
	RelationDirNeutral RelationDir = iota
	RelationDirLeft
	RelationDirRight
)

type Relation struct {
	Entity
	Direction RelationDir
}

func (m *MergeCmd) Parse(p *parser) error {
	p.accept()
	return nil
}

func (m *MergeCmd) Identifier() string {
	return "MERGE"
}

func (m *MergeCmd) Labels() []string {
	return nil
}

func (m *MergeCmd) Attributes() map[string]any {
	return nil
}

func (m *MatchCmd) Parse(p *parser) error {
	p.accept()
	return nil
}

func (m *MatchCmd) Identifier() string {
	return "MATCH"
}

func (m *MatchCmd) Labels() []string {
	return nil
}

func (m *MatchCmd) Attributes() map[string]any {
	return nil
}

func (n *Node) Parse(p *parser) error {
	for {
		i := p.pop()
		switch i.typ {
		case itemNodeIdentifier:
			n.identifier = i.val
			p.accept()
		case itemNodeLabelStart:
			p.accept()
		case itemNodeLabel:
			n.labels = append(n.labels, i.val)
			p.accept()
		case itemAttributesStart:
			err := n.parseAttr(p)
			if err != nil {
				return err
			}
		case itemEndNode:
			return nil
		case itemEOF:
			return ErrUnexpectedEndOfInput
		default:
			return fmt.Errorf("unknown token: %s (%d)", i.val, i.pos)
		}
	}
}

func (r *Relation) Parse(p *parser) error {
	for {
		i := p.pop()
		switch i.typ {
		case itemRelationDirNeutral:
			p.accept()
			return nil
		case itemRelationDirLeft:
			r.Direction = RelationDirLeft
		case itemRelationDirRight:
			r.Direction = RelationDirRight
			p.accept()
			return nil
		case itemRelationStart:
			r.parseInner(p)
		case itemEOF:
			return ErrUnexpectedEndOfInput
		default:
			return fmt.Errorf("unknown token: %s (%d)", i.val, i.pos)
		}
	}
}

func (r *Relation) parseInner(p *parser) error {
	for {
		i := p.pop()
		switch i.typ {
		case itemRelationLabelStart:
			p.accept()
		case itemRelationLabel:
			r.labels = append(r.labels, i.val)
			p.accept()
		case itemAttributesStart:
			err := r.parseAttr(p)
			if err != nil {
				return err
			}
			p.accept()
		case itemRelationEnd:
			p.accept()
			return nil
		case itemEOF:
			return ErrUnexpectedEndOfInput
		default:
			return fmt.Errorf("unknown token: %s (%d)", i.val, i.pos)
		}
	}
}
