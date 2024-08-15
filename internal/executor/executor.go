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

type storeable interface {
	finalise(*sqlx.Tx) (ast.Entity, error)
}

type command interface {
	storeable
	accept(*executor) error
}

type mergeCmd struct {
	items []storeable
}

type matchCmd struct {
	items []storeable
}

type nodeEntity struct {
	node ast.Entity
}

type relationEntity struct {
	relation ast.Relation
}

type executor struct {
	stmt       []ast.Entity
	store      store
	logger     *slog.Logger
	start, pos int
	cmd        command
}

func New(stmt []ast.Entity, s store, logger *slog.Logger) (*executor, error) {
	return &executor{
		stmt:   stmt,
		logger: logger,
		store:  s,
	}, nil
}

func (e *executor) Execute() (ast.Entity, error) {
	for {
		i := e.pop()
		if i == nil {
			if e.stmt == nil {
				return nil, errors.New("no command found")
			}
			return e.Finalise()
		}
		switch i.Type() {
		case ast.EntityTypeMatchCmd:
			if e.cmd != nil {
				return nil, fmt.Errorf("unexpected command: %v", i)
			}
			e.cmd = &matchCmd{
				items: make([]storeable, 0),
			}
			e.cmd.accept(e)
		case ast.EntityTypeMergeCmd:
			if e.cmd != nil {
				return nil, fmt.Errorf("unexpected command: %v", i)
			}
			e.cmd = &mergeCmd{
				items: make([]storeable, 0),
			}
			e.cmd.accept(e)
		default:
			return nil, fmt.Errorf("unexpected token: %v", i)
		}
	}
}

func (e *executor) pop() ast.Entity {
	if e.pos >= len(e.stmt) {
		return nil
	}
	i := e.stmt[e.pos]
	e.pos++
	return i
}

func (e *executor) Finalise() (ast.Entity, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 86400*time.Second)
	defer cancelFn()

	tx, err := e.store.CreateTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating tx: %w", err)
	}

	if e.cmd == nil {
		return nil, errors.New("no command found")
	}
	res, err := e.cmd.finalise(tx)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("finalising node: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("commiting changes: %w", err)
	}

	return res, nil
}

func (e *nodeEntity) finalise(tx *sqlx.Tx) (ast.Entity, error) {
	_, err := e.checkExists(tx)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	_, err = tx.Exec(`insert into nodes(id, created_at) values(?, ?) on conflict(id) do update set updated_at = ?`, e.node.ID(), now, now)
	if err != nil {
		return nil, fmt.Errorf("upserting node: %w", err)
	}

	err = e.finaliseLabels(tx)
	if err != nil {
		return nil, fmt.Errorf("finalising labels: %w", err)
	}

	err = e.finaliseAttributes(tx)
	if err != nil {
		return nil, fmt.Errorf("finalising attrs: %w", err)
	}

	return e, nil
}

func (e *nodeEntity) finaliseLabels(tx *sqlx.Tx) error {
	if len(e.node.Labels()) == 0 {
		return nil
	}

	labels := map[string]string{}
	rows, err := tx.Queryx("select id, label from node_labels where node_id = ?", e.ID())
	if err != nil {
		return fmt.Errorf("querying labels: %w", err)
	}

	for rows.Next() {
		l := struct {
			ID    string `db:"id"`
			Label string `db:"label"`
		}{}
		err = rows.StructScan(&l)
		if err != nil {
			return fmt.Errorf("scanning label: %w", err)
		}
		labels[l.Label] = l.ID
	}

	now := time.Now().UTC()
	for _, l := range e.node.Labels() {
		id := ""
		if lid, ok := labels[l]; ok {
			id = lid
		} else {
			id, err = gonanoid.New()
			if err != nil {
				return fmt.Errorf("label id: %w", err)
			}
		}
		_, err = tx.Exec("insert into node_labels(id, created_at, node_id, label) values(?, ?, ?, ?) on conflict(id) do update set updated_at = ?", id, now, e.ID(), l, now)
		if err != nil {
			return fmt.Errorf("inserting label: %w", err)
		}
		delete(labels, l)
	}

	for _, id := range labels {
		_, err = tx.Exec("delete from node_labels where id = ?", id)
		if err != nil {
			return fmt.Errorf("deleting label: %w", err)
		}
	}

	return nil
}

func (e *nodeEntity) finaliseAttributes(tx *sqlx.Tx) error {
	if len(e.node.Attributes()) == 0 {
		return nil
	}

	attrs := map[string]string{}
	rows, err := tx.Queryx("select id, attr_name from node_attributes where node_id = ?", e.ID())
	if err != nil {
		return fmt.Errorf("querying attrs: %w", err)
	}

	for rows.Next() {
		a := struct {
			ID       string `db:"id"`
			AttrName string `db:"attr_name"`
		}{}
		err = rows.StructScan(&a)
		if err != nil {
			return fmt.Errorf("scanning attr: %w", err)
		}
		attrs[a.AttrName] = a.ID
	}

	now := time.Now().UTC()
	for _, a := range e.node.Attributes() {
		id := ""
		if l, ok := attrs[a.Key()]; ok {
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
			on conflict(id) do update set updated_at = ?, attr_value = ?`, id, now, e.ID(), a.Key, a.Value, a.Type, now, a.Value)
		if err != nil {
			return fmt.Errorf("inserting attr: %w", err)
		}
		delete(attrs, a.Key())
	}

	for _, id := range attrs {
		_, err = tx.Exec("delete from node_attributes where id = ?", id)
		if err != nil {
			return fmt.Errorf("deleting attr: %w", err)
		}
	}

	return nil
}

func (e *nodeEntity) checkExists(tx *sqlx.Tx) (bool, error) {
	args := []any{}
	query := strings.Builder{}
	query.WriteString("select n.id from nodes n\n")

	if val, ok := e.node.Attribute("id"); ok {
		query.WriteString("where n.id = ?")
		args = append(args, val)
	} else {
		i := 0
		for _, v := range e.node.Attributes() {
			query.WriteString(fmt.Sprintf(`
				inner join (select * from node_attributes where attr_name = ? and attr_value = ?) na%d
				on n.id = na%d.node_id
			`, i, i))
			args = append(args, v.Key)
			args = append(args, v.Value)
			i++
		}
		for _, l := range e.node.Labels() {
			query.WriteString(fmt.Sprintf(`
				inner join (select * from node_labels where label = ?) nl%d
				on n.id = nl%d.node_id
			`, i, i))
			args = append(args, l)
			i++
		}
	}

	// TODO: check only one matching row

	id := ""
	isNew := false
	err := tx.Get(&id, query.String(), args...)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("checking existing: %w", err)
		}
		isNew = true
		id, err = gonanoid.New()
		if err != nil {
			return false, fmt.Errorf("generating id: %w", err)
		}
		e.node.WithID(ast.EntityID(id))
	} else {
		e.node.WithID(ast.EntityID(id))
	}

	return !isNew, nil
}

func (e *nodeEntity) ID() ast.EntityID {
	return e.node.ID()
}

func (e *nodeEntity) WithID(id ast.EntityID) ast.Entity {
	return e.node.WithID(id)
}

func (e *nodeEntity) Type() ast.EntityType {
	return ast.EntityTypeNode
}

func (e *nodeEntity) Identifier() string {
	return e.node.Identifier()
}

func (e *nodeEntity) Labels() []string {
	return e.node.Labels()
}

func (e *nodeEntity) Attributes() map[string]ast.Attribute {
	return e.node.Attributes()
}

func (e *nodeEntity) Attribute(k string) (any, bool) {
	return e.node.Attribute(k)
}

func (e *relationEntity) finalise(tx *sqlx.Tx) (ast.Entity, error) {
	_, err := e.left.finalise(tx)
	if err != nil {
		return nil, fmt.Errorf("finalising left node: %w", err)
	}

	_, err = e.right.finalise(tx)
	if err != nil {
		return nil, fmt.Errorf("finalising right node: %w", err)
	}

	_, err = e.checkExists(tx)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	_, err = tx.Exec(`
		insert into relations(id, created_at, left_node_id, right_node_id, direction)
		values(?, ?, ?, ?, ?)
		on conflict(id) do update set
		updated_at = ?,
		left_node_id = ?,
		right_node_id = ?,
		direction = ?`, e.ID(), now, e.left.ID(), e.right.ID(), e.relation.Direction(), now, e.left.ID(), e.right.ID(), e.relation.Direction())
	if err != nil {
		return nil, fmt.Errorf("upserting relation: %w", err)
	}

	err = e.finaliseLabels(tx)
	if err != nil {
		return nil, fmt.Errorf("finalising labels: %w", err)
	}

	err = e.finaliseAttributes(tx)
	if err != nil {
		return nil, fmt.Errorf("finalising attrs: %w", err)
	}

	return e, nil
}

func (e *relationEntity) checkExists(tx *sqlx.Tx) (bool, error) {
	args := []any{}
	query := strings.Builder{}
	query.WriteString("select r.id from relations r\n")

	if val, ok := e.relation.Attribute("id"); ok {
		query.WriteString("where r.id = ?")
		args = append(args, val)
	} else {
		i := 0
		for _, v := range e.relation.Attributes() {
			query.WriteString(fmt.Sprintf(`
				inner join (select * from relation_attributes where attr_name = ? and attr_value = ?) ra%d
				on r.id = ra%d.relation_id
			`, i, i))
			args = append(args, v.Key)
			args = append(args, v.Value)
			i++
		}
		for _, l := range e.relation.Labels() {
			query.WriteString(fmt.Sprintf(`
				inner join (select * from relation_labels where label = ?) rl%d
				on r.id = rl%d.relation_id`, i, i))
			args = append(args, l)
			i++
		}
	}

	// TODO: check only one matching row

	id := ""
	isNew := false
	err := tx.Get(&id, query.String(), args...)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("checking existing: %w", err)
		}
		isNew = true
		id, err := gonanoid.New()
		if err != nil {
			return false, fmt.Errorf("generating id: %w", err)
		}
		e.relation.WithID(ast.EntityID(id))
	} else {
		e.relation.WithID(ast.EntityID(id))
	}

	return !isNew, nil
}

func (e *relationEntity) finaliseLabels(tx *sqlx.Tx) error {
	if len(e.relation.Labels()) == 0 {
		return nil
	}

	labels := map[string]string{}
	rows, err := tx.Queryx("select id, label from relation_labels where relation_id = ?", e.ID())
	if err != nil {
		return fmt.Errorf("querying labels: %w", err)
	}

	for rows.Next() {
		l := struct {
			ID    string `db:"id"`
			Label string `db:"label"`
		}{}
		err = rows.StructScan(&l)
		if err != nil {
			return fmt.Errorf("scanning label: %w", err)
		}
		labels[l.Label] = l.ID
	}

	now := time.Now().UTC()
	for _, l := range e.relation.Labels() {
		id := ""
		if lid, ok := labels[l]; ok {
			id = lid
		} else {
			id, err = gonanoid.New()
			if err != nil {
				return fmt.Errorf("label id: %w", err)
			}
		}
		_, err = tx.Exec(`
			insert into relation_labels(id, created_at, relation_id, label)
			values(?, ?, ?, ?)
			on conflict(id) do update set updated_at = ?`, id, now, e.ID(), l, now)
		if err != nil {
			return fmt.Errorf("inserting label: %w", err)
		}
		delete(labels, l)
	}

	for _, id := range labels {
		_, err = tx.Exec("delete from relation_labels where id = ?", id)
		if err != nil {
			return fmt.Errorf("deleting label: %w", err)
		}
	}

	return nil
}

func (e *relationEntity) finaliseAttributes(tx *sqlx.Tx) error {
	if len(e.relation.Attributes()) == 0 {
		return nil
	}

	attrs := map[string]string{}
	rows, err := tx.Queryx("select id, attr_name from relation_attributes where relation_id = ?", e.ID())
	if err != nil {
		return fmt.Errorf("querying attrs: %w", err)
	}

	for rows.Next() {
		a := struct {
			ID       string `db:"id"`
			AttrName string `db:"attr_name"`
		}{}
		err = rows.StructScan(&a)
		if err != nil {
			return fmt.Errorf("scanning attr: %w", err)
		}
		attrs[a.AttrName] = a.ID
	}

	now := time.Now().UTC()
	for _, a := range e.relation.Attributes() {
		id := ""
		if l, ok := attrs[a.Key()]; ok {
			id = l
		} else {
			id, err = gonanoid.New()
			if err != nil {
				return fmt.Errorf("attr id: %w", err)
			}
		}
		_, err = tx.Exec(`
			insert into relation_attributes(id, created_at, relation_id, attr_name, attr_value, data_type)
			values(?, ?, ?, ?, ?, ?)
			on conflict(id) do update set updated_at = ?, attr_value = ?`, id, now, e.ID(), a.Key, a.Value, a.Type, now, a.Value)
		if err != nil {
			return fmt.Errorf("inserting attr: %w", err)
		}
		delete(attrs, a.Key())
	}

	for _, id := range attrs {
		_, err = tx.Exec("delete from relation_attributes where id = ?", id)
		if err != nil {
			return fmt.Errorf("deleting attr: %w", err)
		}
	}

	return nil
}

func (e *relationEntity) ID() ast.EntityID {
	return e.relation.ID()
}

func (e *relationEntity) WithID(id ast.EntityID) ast.Entity {
	return e.relation.WithID(id)
}

func (e *relationEntity) Type() ast.EntityType {
	return e.relation.Type()
}

func (e *relationEntity) Identifier() string {
	return e.relation.Identifier()
}

func (e *relationEntity) Labels() []string {
	return e.relation.Labels()
}

func (e *relationEntity) Attribute(k string) (any, bool) {
	return e.relation.Attribute(k)
}

func (e *relationEntity) Attributes() map[string]ast.Attribute {
	return e.relation.Attributes()
}

func (m *mergeCmd) finalise(tx *sqlx.Tx) (ast.Entity, error) {
	if len(m.items) == 1 {
		return m.items[0].finalise(tx)
	}

	if _, ok := m.items[0].(*nodeEntity); !ok {
		return nil, errors.New("expected node")
	}

	if _, ok := m.items[2].(*nodeEntity); !ok {
		return nil, errors.New("expected node")
	}

	var rel *relationEntity
	var ok bool
	if rel, ok = m.items[1].(*relationEntity); !ok {
		return nil, errors.New("expected relation")
	}

	rel.left = m.items[0].(*nodeEntity)
	rel.right = m.items[2].(*nodeEntity)
	res, err := rel.finalise(tx)
	if err != nil {
		return nil, fmt.Errorf("finalising relation: %w", err)
	}

	return res, nil
}

func (m *mergeCmd) accept(e *executor) error {
	for {
		i := e.pop()
		if i == nil {
			if !(len(m.items) == 1 || len(m.items) == 3) {
				return fmt.Errorf("unexpected number of entities: %d", len(m.items))
			}
			return nil
		}
		switch i.Type() {
		case ast.EntityTypeNode:
			m.items = append(m.items, &nodeEntity{
				node: i,
			})
		case ast.EntityTypeRelation:
			m.items = append(m.items, &relationEntity{
				relation: i.(ast.Relation),
			})
		default:
			return fmt.Errorf("unexpected entity: %v", i)
		}
	}
}

func (m *matchCmd) finalise(tx *sqlx.Tx) (ast.Entity, error) {
	return nil, nil
}

func (m *matchCmd) accept(e *executor) error {
	return nil
}
