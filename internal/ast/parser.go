package ast

type parser struct {
	items    []item
	start    int
	pos      int
	entities []parseable
}

func Parse(l *lexer) *parser {
	return &parser{
		items:    l.items,
		entities: []parseable{},
	}
}

func (p *parser) Entities() []Entity {
	ents := make([]Entity, len(p.entities))
	for i, v := range p.entities {
		ents[i] = v
	}
	return ents
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
			p.add(cmd)
		case itemMatch:
			cmd, err := p.match()
			if err != nil {
				return err
			}
			p.add(cmd)
		case itemNodeStart:
			n, err := p.node()
			if err != nil {
				return err
			}
			p.add(n)
		case itemRelationDirNeutral:
			fallthrough
		case itemRelationDirLeft:
			r, err := p.relation()
			if err != nil {
				return err
			}
			p.add(r)
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

func (p *parser) accept() []item {
	res := p.items[p.start:p.pos]
	p.start = p.pos
	return res
}

func (p *parser) add(n parseable) {
	p.entities = append(p.entities, n)
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
