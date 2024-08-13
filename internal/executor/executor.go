package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jdudmesh/propolis/internal/ast"
	"github.com/jmoiron/sqlx"
	gonanoid "github.com/matoous/go-nanoid/v2"
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

func (e *executor) Finalise() error {
	return e.finalise()
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
	_, err := e.checkExists(tx)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	_, err = tx.Exec(`insert into nodes(id, created_at) values(?, ?) on conflict(id) do update set updated_at = ?`, e.id, now, now)
	if err != nil {
		return fmt.Errorf("upserting node: %w", err)
	}

	err = e.finaliseLabels(tx)
	if err != nil {
		return fmt.Errorf("finalising labels: %w", err)
	}

	err = e.finaliseAttributes(tx)
	if err != nil {
		return fmt.Errorf("finalising attrs: %w", err)
	}

	return nil
}

func (e *NodeEntity) finaliseLabels(tx *sqlx.Tx) error {
	if len(e.node.Labels()) == 0 {
		return nil
	}

	labels := map[string]string{}
	rows, err := tx.Queryx("select id, label from node_labels where node_id = ?", e.id)
	if err != nil {
		return fmt.Errorf("querying labels: %w", err)
	}

	for rows.Next() {
		s := struct {
			ID    string `db:"id"`
			Label string `db:"label"`
		}{}
		err = rows.StructScan(&s)
		if err != nil {
			return fmt.Errorf("scanning label: %w", err)
		}
		labels[s.Label] = s.ID
	}

	now := time.Now().UTC()
	for _, label := range e.node.Labels() {
		id := ""
		if l, ok := labels[label]; ok {
			id = l
		} else {
			id, err = gonanoid.New()
			if err != nil {
				return fmt.Errorf("label id: %w", err)
			}
		}
		_, err = tx.Exec("insert into node_labels(id, created_at, node_id, label) values(?, ?, ?, ?) on conflict(id) do update set updated_at = ?", id, now, e.id, label, now)
		if err != nil {
			return fmt.Errorf("inserting label: %w", err)
		}
		delete(labels, id)
	}

	for _, id := range labels {
		_, err = tx.Exec("delete from node_labels where id = ?", id)
		if err != nil {
			return fmt.Errorf("deleting label: %w", err)
		}
	}

	return nil
}

func (e *NodeEntity) finaliseAttributes(tx *sqlx.Tx) error {
	if len(e.node.Attributes()) == 0 {
		return nil
	}

	attrs := map[string]string{}
	rows, err := tx.Queryx("select id, attr_name from node_attributes where node_id = ?", e.id)
	if err != nil {
		return fmt.Errorf("querying attrs: %w", err)
	}

	for rows.Next() {
		s := struct {
			ID       string `db:"id"`
			AttrName string `db:"attr_name"`
		}{}
		err = rows.StructScan(&s)
		if err != nil {
			return fmt.Errorf("scanning attr: %w", err)
		}
		attrs[s.AttrName] = s.ID
	}

	now := time.Now().UTC()
	for _, v := range e.node.Attributes() {
		id := ""
		if l, ok := attrs[v.Key]; ok {
			id = l
		} else {
			id, err = gonanoid.New()
			if err != nil {
				return fmt.Errorf("attr id: %w", err)
			}
		}
		_, err = tx.Exec(`
			insert into node_attributes(id, created_at, node_id, attr_name, attr_value, data_type)
			values(?, ?, ?, ?, ?, ?)
			on conflict(id) do update set updated_at = ?, attr_value = ?`, id, now, e.id, v.Key, v.Value, v.Type, now, v.Value)
		if err != nil {
			return fmt.Errorf("inserting attr: %w", err)
		}
		delete(attrs, id)
	}

	for _, id := range attrs {
		_, err = tx.Exec("delete from node_attributes where id = ?", id)
		if err != nil {
			return fmt.Errorf("deleting attr: %w", err)
		}
	}

	return nil
}

func (e *NodeEntity) checkExists(tx *sqlx.Tx) (bool, error) {
	args := []any{}
	query := strings.Builder{}
	query.WriteString("select n.id from nodes n\n")

	if val, ok := e.node.Attribute("id"); ok {
		query.WriteString("where n.id = ?")
		args = append(args, val)
	} else {
		i := 0
		for _, v := range e.node.Attributes() {
			query.WriteString(fmt.Sprintf("inner join (select * from node_attributes where attr_name = ? and attr_value = ?) na%d on n.id = na%d.node_id\n", i, i))
			args = append(args, v.Key)
			args = append(args, v.Value)
			i++
		}
		for l := range e.node.Labels() {
			query.WriteString(fmt.Sprintf("inner join (select * from node_labels where label = ?) nl%d on n.id = nl%d.node_id\n", i, i))
			args = append(args, l)
			i++
		}
	}

	fmt.Println(query.String())
	// TODO: check only one matching row

	id := ""
	isNew := false
	err := tx.Get(&id, query.String(), args...)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("checking existing: %w", err)
		}
		isNew = true
		e.id, err = gonanoid.New()
		if err != nil {
			return false, fmt.Errorf("generating id: %w", err)
		}
	} else {
		e.id = id
	}

	return !isNew, nil
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
	ctx, cancelFn := context.WithTimeout(context.Background(), 86400*time.Second)
	defer cancelFn()

	tx, err := s.CreateTx(ctx)
	if err != nil {
		return fmt.Errorf("creating tx: %w", err)
	}

	if len(m.items) == 1 {
		err := m.items[0].Finalise(tx)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("finalising node: %w", err)
		}
		return nil
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
			tx.Rollback()
			return fmt.Errorf("finalising relation: %w", err)
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
