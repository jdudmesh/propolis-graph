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
	"errors"
	"fmt"
)

var (
	ErrUnexpectedEndOfInput = errors.New("unexpected end of input")
)

type AttributeDataType int

const (
	AttributeDataTypeNumber AttributeDataType = iota
	AttributeDataTypeString
)

type Attribute interface {
	Key() string
	Value() string
	Type() AttributeDataType
}

type Entity interface {
	Type() EntityType
	Identifier() string
	Labels() []string
	Attributes() map[string]Attribute
	Attribute(string) (string, bool)
}

type Relation interface {
	Entity
	Direction() RelationDir
	Left() Entity
	Right() Entity
}

type Command interface {
	Entity() Entity
}

type parseable interface {
	Entity
	parse(p *parser) error
}

type mergeCmd struct {
	entity Entity
}

type matchCmd struct {
	entity Entity
}

type EntityID string
type EntityType int

const EntityIDNil = EntityID("")

const (
	EntityTypeNode EntityType = iota
	EntityTypeRelation
	EntityTypeMergeCmd
	EntityTypeMatchCmd
)

type entity struct {
	typ        EntityType
	identifier string
	labels     []string
	attributes map[string]Attribute
}

type node struct {
	entity
}

type RelationDir int

const (
	RelationDirNeutral RelationDir = iota
	RelationDirLeft
	RelationDirRight
)

type relation struct {
	entity
	direction RelationDir
	left      Entity
	right     Entity
}

type attribute struct {
	key   string
	value string
	typ   AttributeDataType
}

func (e entity) Type() EntityType {
	return e.typ
}

func (e entity) Identifier() string {
	return e.identifier
}

func (e entity) Labels() []string {
	return e.labels
}

func (e entity) Attributes() map[string]Attribute {
	return e.attributes
}

func (e entity) Attribute(k string) (string, bool) {
	if val, ok := e.attributes[k]; ok {
		return val.Value(), true
	} else {
		return "", false
	}
}

func (e *entity) parseAttr(p *parser) error {
	attribKey := ""
	for {
		i := p.pop()
		switch i.typ {
		case itemAttributesEnd:
			p.accept()
			return nil
		case itemAttribSeparator:
			p.accept()
		case itemAttribIdentifier:
			attribKey = i.val
		case itemAttribValue:
			if attribKey == "" {
				return fmt.Errorf("unexpected input: %s (%d)", i.val, i.pos)
			}
			dataType := AttributeDataTypeNumber
			attribValue := i.val
			if attribValue[0] == '\'' && attribValue[len(attribValue)-1] == '\'' {
				dataType = AttributeDataTypeString
				attribValue = attribValue[1 : len(attribValue)-1]
			}
			e.attributes[attribKey] = &attribute{
				key:   attribKey,
				value: attribValue,
				typ:   dataType,
			}
			attribKey = ""
		case itemEOF:
			return ErrUnexpectedEndOfInput
		default:
			return fmt.Errorf("unknown token: %s (%d)", i.val, i.pos)
		}
	}
}

func (m *mergeCmd) parse(p *parser) error {
	p.accept()
	for {
		i := p.pop()
		switch i.typ {
		case itemEOF:
			return nil
		case itemNodeStart:
			n, err := p.node()
			if err != nil {
				return err
			}
			if m.entity == nil {
				m.entity = n
				continue
			}
			if r, ok := m.entity.(*relation); !ok {
				return fmt.Errorf("unexpected entity: %v", n)
			} else {
				r.right = n
			}
		case itemRelationDirNeutral:
			fallthrough
		case itemRelationDirLeft:
			r, err := p.relation()
			if err != nil {
				return err
			}
			if n, ok := m.entity.(*node); !ok {
				return fmt.Errorf("unexpected entity: %v", n)
			} else {
				m.entity = r
				r.left = n
			}
		default:
			return fmt.Errorf("unexpected item: %v", i)
		}
	}
}

func (m *mergeCmd) Type() EntityType {
	return EntityTypeMergeCmd
}

func (m *mergeCmd) Identifier() string {
	return "MERGE"
}

func (m *mergeCmd) Labels() []string {
	return nil
}

func (m *mergeCmd) Attributes() map[string]Attribute {
	return nil
}

func (m *mergeCmd) Attribute(k string) (string, bool) {
	return "", false
}

func (m *matchCmd) parse(p *parser) error {
	p.accept()
	for {
		i := p.pop()
		switch i.typ {
		case itemEOF:
			return nil
		case itemNodeStart:
			n, err := p.node()
			if err != nil {
				return err
			}
			// TODO: for now we will only allow matches on single nodes
			if m.entity != nil {
				return fmt.Errorf("unexpected entity: %v", n)
			}
			m.entity = n
		default:
			return fmt.Errorf("unexpected item: %v", i)
		}
	}
}

func (m *matchCmd) Type() EntityType {
	return EntityTypeMatchCmd
}

func (m *matchCmd) Identifier() string {
	return "MATCH"
}

func (m *matchCmd) Labels() []string {
	return nil
}

func (m *matchCmd) Attributes() map[string]Attribute {
	return nil
}

func (m *matchCmd) Attribute(k string) (string, bool) {
	return "", false
}

func (n *node) Type() EntityType {
	return EntityTypeNode
}

func (n *node) Identifier() string {
	return "MATCH"
}

func (n *node) Labels() []string {
	return n.labels
}

func (n *node) Attributes() map[string]Attribute {
	return n.attributes
}

func (n *node) parse(p *parser) error {
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

func (r *relation) Type() EntityType {
	return EntityTypeRelation
}

func (r *relation) Identifier() string {
	return "MATCH"
}

func (r *relation) Labels() []string {
	return r.labels
}

func (r *relation) Attributes() map[string]Attribute {
	return r.attributes
}

func (r *relation) Direction() RelationDir {
	return r.direction
}

func (r *relation) Left() Entity {
	return r.left
}

func (r *relation) Right() Entity {
	return r.right
}

func (r *relation) parse(p *parser) error {
	for {
		i := p.pop()
		switch i.typ {
		case itemRelationDirNeutral:
			p.accept()
			return nil
		case itemRelationDirLeft:
			r.direction = RelationDirLeft
		case itemRelationDirRight:
			r.direction = RelationDirRight
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

func (r *relation) parseInner(p *parser) error {
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

func (a attribute) Key() string {
	return a.key
}

func (a attribute) Value() string {
	return a.value
}

func (a attribute) Type() AttributeDataType {
	return a.typ
}

func (c mergeCmd) Entity() Entity {
	return c.entity
}

func (c matchCmd) Entity() Entity {
	return c.entity
}
