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

type executor struct {
	stmt       ast.Entity
	store      store
	logger     *slog.Logger
	start, pos int
}

func New(stmt ast.Entity, s store, logger *slog.Logger) (*executor, error) {
	return &executor{
		stmt:   stmt,
		logger: logger,
		store:  s,
	}, nil
}

func (e *executor) Execute() (ast.Entity, error) {
	if e.stmt == nil {
		return nil, errors.New("no command found")
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), 86400*time.Second)
	defer cancelFn()

	tx, err := e.store.CreateTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating tx: %w", err)
	}

	var res ast.Entity
	switch e.stmt.Type() {
	case ast.EntityTypeMergeCmd:
		res, err = e.finaliseMergeCmd(e.stmt.(ast.Command), tx)
	case ast.EntityTypeMatchCmd:
		res, err = e.finaliseMatchCmd(e.stmt.(ast.Command), tx)
	}

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

func (e *executor) finaliseNode(n ast.Entity, tx *sqlx.Tx) error {
	_, err := e.checkNodeExists(n, tx)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	_, err = tx.Exec(`insert into nodes(id, created_at) values(?, ?) on conflict(id) do update set updated_at = ?`, n.ID(), now, now)
	if err != nil {
		return fmt.Errorf("upserting node: %w", err)
	}

	err = e.finaliseNodeLabels(n, tx)
	if err != nil {
		return fmt.Errorf("finalising labels: %w", err)
	}

	err = e.finaliseNodeAttributes(n, tx)
	if err != nil {
		return fmt.Errorf("finalising attrs: %w", err)
	}

	return nil
}

func (e *executor) finaliseNodeLabels(n ast.Entity, tx *sqlx.Tx) error {
	if len(n.Labels()) == 0 {
		return nil
	}

	labels := map[string]string{}
	rows, err := tx.Queryx("select id, label from node_labels where node_id = ?", n.ID())
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
	for _, l := range n.Labels() {
		id := ""
		if lid, ok := labels[l]; ok {
			id = lid
		} else {
			id, err = gonanoid.New()
			if err != nil {
				return fmt.Errorf("label id: %w", err)
			}
		}
		_, err = tx.Exec("insert into node_labels(id, created_at, node_id, label) values(?, ?, ?, ?) on conflict(id) do update set updated_at = ?", id, now, n.ID(), l, now)
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

func (e *executor) finaliseNodeAttributes(n ast.Entity, tx *sqlx.Tx) error {
	if len(n.Attributes()) == 0 {
		return nil
	}

	attrs := map[string]string{}
	rows, err := tx.Queryx("select id, attr_name from node_attributes where node_id = ?", n.ID())
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
	for _, a := range n.Attributes() {
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
			on conflict(id) do update set updated_at = ?, attr_value = ?`, id, now, n.ID(), a.Key, a.Value, a.Type, now, a.Value)
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

func (e *executor) checkNodeExists(n ast.Entity, tx *sqlx.Tx) (bool, error) {
	args := []any{}
	query := strings.Builder{}
	query.WriteString("select n.id from nodes n\n")

	if val, ok := n.Attribute("id"); ok {
		query.WriteString("where n.id = ?")
		args = append(args, val)
	} else {
		i := 0
		for _, v := range n.Attributes() {
			query.WriteString(fmt.Sprintf(`
				inner join (select * from node_attributes where attr_name = ? and attr_value = ?) na%d
				on n.id = na%d.node_id
			`, i, i))
			args = append(args, v.Key)
			args = append(args, v.Value)
			i++
		}
		for _, l := range n.Labels() {
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
	}

	n.WithID(ast.EntityID(id))

	return !isNew, nil
}

func (e *executor) finaliseRelation(r ast.Relation, tx *sqlx.Tx) error {
	err := e.finaliseNode(r.Left(), tx)
	if err != nil {
		return fmt.Errorf("finalising left node: %w", err)
	}

	err = e.finaliseNode(r.Right(), tx)
	if err != nil {
		return fmt.Errorf("finalising right node: %w", err)
	}

	_, err = e.checkRelationExists(r, tx)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	_, err = tx.Exec(`
		insert into relations(id, created_at, left_node_id, right_node_id, direction)
		values(?, ?, ?, ?, ?)
		on conflict(id) do update set
		updated_at = ?,
		left_node_id = ?,
		right_node_id = ?,
		direction = ?`, r.ID(), now, r.Left().ID(), r.Right().ID(), r.Direction(), now, r.Left().ID(), r.Right().ID(), r.Direction())
	if err != nil {
		return fmt.Errorf("upserting relation: %w", err)
	}

	err = e.finaliseRelationLabels(r, tx)
	if err != nil {
		return fmt.Errorf("finalising labels: %w", err)
	}

	err = e.finaliseRelationAttributes(r, tx)
	if err != nil {
		return fmt.Errorf("finalising attrs: %w", err)
	}

	return nil
}

func (e *executor) checkRelationExists(r ast.Relation, tx *sqlx.Tx) (bool, error) {
	args := []any{}
	query := strings.Builder{}
	query.WriteString("select r.id from relations r\n")

	if val, ok := r.Attribute("id"); ok {
		query.WriteString("where r.id = ?")
		args = append(args, val)
	} else {
		i := 0
		for _, v := range r.Attributes() {
			query.WriteString(fmt.Sprintf(`
				inner join (select * from relation_attributes where attr_name = ? and attr_value = ?) ra%d
				on r.id = ra%d.relation_id
			`, i, i))
			args = append(args, v.Key)
			args = append(args, v.Value)
			i++
		}
		for _, l := range r.Labels() {
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
		id, err = gonanoid.New()
		if err != nil {
			return false, fmt.Errorf("generating id: %w", err)
		}
	}
	r.WithID(ast.EntityID(id))

	return !isNew, nil
}

func (e *executor) finaliseRelationLabels(r ast.Relation, tx *sqlx.Tx) error {
	if len(r.Labels()) == 0 {
		return nil
	}

	labels := map[string]string{}
	rows, err := tx.Queryx("select id, label from relation_labels where relation_id = ?", r.ID())
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
	for _, l := range r.Labels() {
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
			on conflict(id) do update set updated_at = ?`, id, now, r.ID(), l, now)
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

func (e *executor) finaliseRelationAttributes(r ast.Relation, tx *sqlx.Tx) error {
	if len(r.Attributes()) == 0 {
		return nil
	}

	attrs := map[string]string{}
	rows, err := tx.Queryx("select id, attr_name from relation_attributes where relation_id = ?", r.ID())
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
	for _, a := range r.Attributes() {
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
			on conflict(id) do update set updated_at = ?, attr_value = ?`, id, now, r.ID(), a.Key, a.Value, a.Type, now, a.Value)
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

func (e *executor) finaliseMergeCmd(mergeCmd ast.Command, tx *sqlx.Tx) (ast.Entity, error) {
	switch mergeCmd.Entity().Type() {
	case ast.EntityTypeNode:
		return mergeCmd.Entity(), e.finaliseNode(mergeCmd.Entity(), tx)
	case ast.EntityTypeRelation:
		return mergeCmd.Entity(), e.finaliseRelation(mergeCmd.Entity().(ast.Relation), tx)
	default:
		return nil, fmt.Errorf("unknown entity: %v", mergeCmd.Entity())
	}
}

func (e *executor) finaliseMatchCmd(cmd ast.Command, tx *sqlx.Tx) (ast.Entity, error) {
	return nil, nil
}
