package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jdudmesh/propolis/internal/ast"
	"github.com/jmoiron/sqlx"
)

type store interface {
	CreateTx(ctx context.Context) (*sqlx.Tx, error)
}
type command interface {
	Finalise(store) error
	Accept(*executor) error
}

type Storeable interface {
	Finalise(*sqlx.Tx) error
	// Insert() error
	// Delete() error
}

type MergeCmd struct {
	items []Storeable
}

type MatchCmd struct {
	items []Storeable
}

type NodeEntity struct {
	id   string
	node *ast.Node
}
type RelationEntity struct {
	id       string
	relation *ast.Relation
	left     *NodeEntity
	right    *NodeEntity
}

type executor struct {
	stmt       []ast.ParseableEntity
	store      store
	logger     *slog.Logger
	start, pos int
	cmd        command
}

func New(stmt []ast.ParseableEntity, s store, logger *slog.Logger) (*executor, error) {
	return &executor{
		stmt:   stmt,
		logger: logger,
		store:  s,
	}, nil
}

func (e *executor) Execute() error {
	for {
		i := e.pop()
		if i == nil {
			if e.stmt == nil {
				return errors.New("no command found")
			}
			return e.finalise()
		}
		switch t := i.(type) {
		case *ast.MatchCmd:
			if e.cmd != nil {
				return fmt.Errorf("unexpected command: %v", t)
			}
			e.cmd = &MatchCmd{
				items: make([]Storeable, 0),
			}
			e.cmd.Accept(e)
		case *ast.MergeCmd:
			if e.cmd != nil {
				return fmt.Errorf("unexpected command: %v", t)
			}
			e.cmd = &MergeCmd{
				items: make([]Storeable, 0),
			}
			e.cmd.Accept(e)
		default:
			return fmt.Errorf("unexpected token: %v", t)
		}
	}
}

func (e *executor) pop() ast.ParseableEntity {
	if e.pos >= len(e.stmt) {
		return nil
	}
	i := e.stmt[e.pos]
	e.pos++
	return i
}

func (e *executor) finalise() error {
	if e.cmd == nil {
		return errors.New("no command found")
	}
	return e.cmd.Finalise(e.store)
}

func (e *NodeEntity) Finalise(tx *sqlx.Tx) error {
	//tx.Exec()
	return nil
}

func (e *RelationEntity) Finalise(tx *sqlx.Tx) error {
	err := e.left.Finalise(tx)
	if err != nil {
		return fmt.Errorf("finalising left node: %w", err)
	}

	err = e.right.Finalise(tx)
	if err != nil {
		return fmt.Errorf("finalising right node: %w", err)
	}

	return nil
}

func (m *MergeCmd) Finalise(s store) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second)
	defer cancelFn()

	tx, err := s.CreateTx(ctx)
	if err != nil {
		return fmt.Errorf("creating tx: %w", err)
	}

	if len(m.items) == 1 {
		err := m.items[0].Finalise(tx)
		if err != nil {
			return tx.Rollback()
		}
	}

	if _, ok := m.items[0].(*NodeEntity); !ok {
		return errors.New("expected node")
	}

	if _, ok := m.items[2].(*NodeEntity); !ok {
		return errors.New("expected node")
	}

	if r, ok := m.items[1].(*RelationEntity); !ok {
		return errors.New("expected relation")
	} else {
		r.left = m.items[0].(*NodeEntity)
		r.right = m.items[2].(*NodeEntity)
		err := r.Finalise(tx)
		if err != nil {
			return tx.Rollback()
		}
	}

	return tx.Commit()
}

func (m *MergeCmd) Accept(e *executor) error {
	for {
		i := e.pop()
		if i == nil {
			if !(len(m.items) == 1 || len(m.items) == 3) {
				return fmt.Errorf("unexpected number of entities: %d", len(m.items))
			}
			return nil
		}
		switch v := i.(type) {
		case *ast.Node:
			m.items = append(m.items, &NodeEntity{
				node: i.(*ast.Node),
			})
		case *ast.Relation:
			m.items = append(m.items, &RelationEntity{
				relation: i.(*ast.Relation),
			})
		default:
			return fmt.Errorf("unexpected entity: %s", v)
		}
	}
}

func (m *MatchCmd) Finalise(s store) error {
	return nil
}

func (m *MatchCmd) Accept(e *executor) error {
	return nil
}
