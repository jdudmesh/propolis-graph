package ast

type parser struct {
	items    []item
	start    int
	pos      int
	entities []ParseableEntity
}

func Parse(l *lexer) *parser {
	return &parser{
		items:    l.items,
		entities: []ParseableEntity{},
	}
}

func (p *parser) Entities() []ParseableEntity {
	return p.entities
}

func (p *parser) Run() error {
	for {
		var err error
		i := p.pop()
		switch i.typ {
		case itemMerge:
			err = p.merge()
		case itemNodeStart:
			err = p.node()
		case itemRelationDirNeutral:
			fallthrough
		case itemRelationDirLeft:
			err = p.relation()
		case itemEOF:
			return nil
		}
		if err != nil {
			return err
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

func (p *parser) add(n ParseableEntity) {
	p.entities = append(p.entities, n)
}

func (p *parser) merge() error {
	m := &MergeCmd{}
	err := m.Parse(p)
	if err != nil {
		return err
	}

	p.add(m)

	return nil
}

func (p *parser) match() error {
	m := &MatchCmd{}
	err := m.Parse(p)
	if err != nil {
		return err
	}

	p.add(m)

	return nil
}

func (p *parser) node() error {
	n := &Node{
		Entity: Entity{
			labels:     []string{},
			attributes: map[string]any{},
		},
	}

	err := n.Parse(p)
	if err != nil {
		return err
	}

	p.add(n)

	return nil
}

func (p *parser) relation() error {
	r := &Relation{
		Entity: Entity{
			labels:     []string{},
			attributes: map[string]any{},
		},
	}
	err := r.Parse(p)
	if err != nil {
		return err
	}

	p.add(r)

	return nil
}
