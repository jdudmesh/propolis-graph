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
	"fmt"
)

type parser struct {
	items []item
	start int
	pos   int
	cmd   Command
}

func Parse(l *lexer) *parser {
	return &parser{
		items: l.items,
	}
}

func (p *parser) Command() Command {
	return p.cmd
}

func (p *parser) Run() error {
	for {
		i := p.pop()
		switch i.typ {
		case itemMerge:
			cmd, err := p.merge()
			if err != nil {
				return err
			}
			p.cmd = cmd
		case itemMatch:
			cmd, err := p.match()
			if err != nil {
				return err
			}
			p.cmd = cmd
		case itemSince:
			if p.cmd == nil {
				return fmt.Errorf("unexpected token: %s", i.val)
			}
			if m, ok := p.cmd.(*matchCmd); !ok {
				return fmt.Errorf("syntax error: since not acceptable")
			} else {
				s, err := p.since()
				if err != nil {
					return err
				}
				m.since = s
			}
		case itemEOF:
			return nil
		}
	}
}

func (p *parser) pop() item {
	if p.pos >= len(p.items) {
		return item{
			typ: itemEOF,
		}
	}
	i := p.items[p.pos]
	p.pos++
	return i
}

func (p *parser) back() {
	p.pos--
}

func (p *parser) accept() []item {
	res := p.items[p.start:p.pos]
	p.start = p.pos
	return res
}

func (p *parser) merge() (*mergeCmd, error) {
	m := &mergeCmd{}
	err := m.parse(p)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (p *parser) match() (*matchCmd, error) {
	m := &matchCmd{}
	err := m.parse(p)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (p *parser) node() (*node, error) {
	n := &node{
		entity: entity{
			labels:     []string{},
			attributes: map[string]Attribute{},
		},
	}

	err := n.parse(p)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (p *parser) relation() (*relation, error) {
	r := &relation{
		entity: entity{
			labels:     []string{},
			attributes: map[string]Attribute{},
		},
	}
	err := r.parse(p)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (p *parser) since() (*sinceClause, error) {
	s := &sinceClause{}
	err := s.parse(p)
	if err != nil {
		return nil, err
	}

	return s, nil
}
