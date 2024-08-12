package ast

import (
	"errors"
	"fmt"
)

type node interface {
	Parse(p *parser) error
	Execute() error
}

type Merge struct {
}

type Node struct {
	identifier string
	labels     []string
	attributes map[string]any
}

type RelationDir int

const (
	RelationDirNeutral RelationDir = iota
	RelationDirLeft
	RelationDirRight
)

type Relation struct {
	dir RelationDir
}

func (m *Merge) Parse(p *parser) error {
	p.accept()
	return nil
}

func (m *Merge) Execute() error {
	return nil
}

func (n *Node) Parse(p *parser) error {
outer:
	for {
		i := p.pop()
		switch i.typ {
		case itemEOF:
			return errors.New("unexpected end of input")
		case itemNodeIdentifier:
			n.identifier = i.val
		case itemNodeLabel:
			n.labels = append(n.labels, i.val)
		case itemAttributesStart:
			n.parseAttr(p)
		case itemEndNode:
			break outer
		}
	}

	def := p.accept()
	for _, x := range def {
		fmt.Printf("%02d: %s", x.typ, x.val)
	}

	return nil
}

func (n *Node) parseAttr(p *parser) error {
	pendingVar := ""
	for {
		i := p.pop()
		switch i.typ {
		case itemEOF:
			return errors.New("unexpected end of input")
		case itemAttributesEnd:
			p.accept()
			return nil
		case itemAttribSeparator:
			continue
		case itemAttribIdentifier:
			pendingVar = i.val
		case itemAttribValue:
			if pendingVar == "" {
				return errors.New("unexpected token")
			}
			n.attributes[pendingVar] = i.val
			fallthrough
		default:
			pendingVar = ""
		}
	}
}

func (n *Node) Execute() error {
	return nil
}

func (r *Relation) Parse(p *parser) error {
	for {
		i := p.pop()
		switch i.typ {
		case itemEOF:
			return errors.New("unexpected end of input")
		case itemRelationDirNeutral:
			p.accept()
			return nil
		case itemRelationDirLeft:
			r.dir = RelationDirLeft
		case itemRelationDirRight:
			r.dir = RelationDirRight
			p.accept()
			return nil
		case itemRelationStart:
			r.parseInner(p)
		}
	}
	return nil
}

func (r *Relation) parseInner(p *parser) error {
	for {
		i := p.pop()
		switch i.typ {
		case itemRelationEnd:
			p.accept()
			return nil
		}
	}
	return nil
}

func (r *Relation) Execute() error {
	return nil
}
