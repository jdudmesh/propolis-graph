package ast

type parser struct {
	items []item
	start int
	pos   int
	nodes []node
}

func parse(l *lexer) *parser {
	return &parser{
		items: l.items,
		nodes: []node{},
	}
}

func (p *parser) run() error {
	for {
		var err error
		i := p.pop()
		switch i.typ {
		case itemMerge:
			err = p.merge()
		case itemNodeStart:
			err = p.node()
		case itemRelationDirNeutral:
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

func (p *parser) peek() item {
	return p.items[p.pos]
}

func (p *parser) addNode(n node) {
	p.nodes = append(p.nodes, n)
}

func (p *parser) merge() error {
	m := &Merge{}
	err := m.Parse(p)
	if err != nil {
		return err
	}

	p.addNode(m)

	return nil
}

func (p *parser) node() error {
	n := &Node{
		labels:     []string{},
		attributes: map[string]any{},
	}

	err := n.Parse(p)
	if err != nil {
		return err
	}

	p.addNode(n)

	return nil
}

func (p *parser) relation() error {
	r := &Relation{}
	err := r.Parse(p)
	if err != nil {
		return err
	}

	p.addNode(r)

	return nil
}
