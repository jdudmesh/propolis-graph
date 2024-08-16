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

var (
	ErrNotFound = errors.New("not found")
)

type store interface {
	CreateTx(ctx context.Context) (*sqlx.Tx, error)
}

type executor struct {
	stmt   any
	store  store
	logger *slog.Logger
}

func New(stmt any, s store, logger *slog.Logger) (*executor, error) {
	return &executor{
		stmt:   stmt,
		logger: logger,
		store:  s,
	}, nil
}

func (e *executor) Execute() (any, error) {
	if e.stmt == nil {
		return nil, errors.New("no command found")
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), 86400*time.Second)
	defer cancelFn()

	tx, err := e.store.CreateTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating tx: %w", err)
	}

	var res any
	if cmd, ok := e.stmt.(ast.Command); ok {
		switch cmd.Type() {
		case ast.EntityTypeMergeCmd:
			res, err = e.finaliseMergeCmd(cmd, tx)
		case ast.EntityTypeMatchCmd:
			res, err = e.finaliseMatchCmd(cmd, tx)
		default:
			return nil, fmt.Errorf("unknown command: %v", cmd)
		}
	} else {
		return nil, fmt.Errorf("unexpected entity: %v", e.stmt)
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

func (e *executor) finaliseNode(n ast.Entity, tx *sqlx.Tx) (*Node, error) {
	now := time.Now().UTC()

	node, err := e.findNode(n, tx)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}

	if node == nil {
		id, err := gonanoid.New()
		if err != nil {
			return nil, fmt.Errorf("node id: %w", err)
		}
		node = &Node{
			ID:        id,
			CreatedAt: now,
		}
	} else {
		node.UpdatedAt = &now
	}

	_, err = tx.NamedExec(`
		insert into nodes(id, created_at)
		values(:id, :created_at)
		on conflict(id) do update
		set updated_at = :updated_at`, node)
	if err != nil {
		return nil, fmt.Errorf("upserting node: %w", err)
	}

	node.labels, err = e.finaliseNodeLabels(node.ID, n, tx)
	if err != nil {
		return nil, fmt.Errorf("finalising labels: %w", err)
	}

	node.attributes, err = e.finaliseNodeAttributes(node.ID, n, tx)
	if err != nil {
		return nil, fmt.Errorf("finalising attrs: %w", err)
	}

	return node, nil
}

func (e *executor) finaliseNodeLabels(nodeID string, n ast.Entity, tx *sqlx.Tx) ([]*NodeLabel, error) {
	now := time.Now().UTC()
	labels := []*NodeLabel{}

	if len(n.Labels()) == 0 {
		return labels, nil
	}

	err := tx.Select(&labels, "select * from node_labels where node_id = ?", nodeID)
	if err != nil {
		return nil, fmt.Errorf("querying labels: %w", err)
	}

	existing := map[string]*NodeLabel{}
	for _, v := range labels {
		existing[v.Label] = v
	}

	for _, l := range n.Labels() {
		label := existing[l]
		if label == nil {
			id, err := gonanoid.New()
			if err != nil {
				return nil, fmt.Errorf("label id: %w", err)
			}
			label = &NodeLabel{
				ID:        id,
				CreatedAt: now,
				NodeID:    nodeID,
				Label:     l,
			}
			labels = append(labels, label)
		} else {
			label.UpdatedAt = &now
		}

		_, err = tx.NamedExec(`
			insert into node_labels(id, created_at, node_id, label)
			values(:id, :created_at, :node_id, :label)
			on conflict(id) do update set updated_at = :updated_at`, label)
		if err != nil {
			return nil, fmt.Errorf("inserting label: %w", err)
		}
		delete(existing, l)
	}

	for _, label := range existing {
		_, err = tx.Exec("delete from node_labels where id = ?", label.ID)
		if err != nil {
			return nil, fmt.Errorf("deleting label: %w", err)
		}
	}

	labels2 := make([]*NodeLabel, 0, len(labels))
	for _, l := range labels {
		if _, ok := existing[l.Label]; ok {
			continue
		}
		labels2 = append(labels2, l)
	}

	return labels2, nil
}

func (e *executor) finaliseNodeAttributes(nodeID string, n ast.Entity, tx *sqlx.Tx) ([]*NodeAttribute, error) {
	now := time.Now().UTC()
	attrs := []*NodeAttribute{}

	if len(n.Attributes()) == 0 {
		return attrs, nil
	}

	err := tx.Select(&attrs, "select * from node_attributes where node_id = ?", nodeID)
	if err != nil {
		return nil, fmt.Errorf("querying attrs: %w", err)
	}

	existing := map[string]*NodeAttribute{}
	for _, a := range attrs {
		existing[a.Name] = a
	}

	for _, a := range n.Attributes() {
		attr := existing[a.Key()]
		if attr == nil {
			id, err := gonanoid.New()
			if err != nil {
				return nil, fmt.Errorf("attr id: %w", err)
			}
			attr = &NodeAttribute{
				ID:        id,
				CreatedAt: now,
				NodeID:    nodeID,
				Name:      a.Key(),
			}
			attrs = append(attrs, attr)
		} else {
			attr.UpdatedAt = &now
		}
		attr.Value = a.Value()
		_, err = tx.NamedExec(`
			insert into node_attributes(id, created_at, node_id, attr_name, attr_value, data_type)
			values(:id, :created_at, :node_id, :attr_name, :attr_value, :data_type)
			on conflict(id) do update set updated_at = :updated_at, attr_value = :attr_value`, &attr)
		if err != nil {
			return nil, fmt.Errorf("inserting attr: %w", err)
		}
		delete(existing, a.Key())
	}

	for _, id := range existing {
		_, err = tx.Exec("delete from node_attributes where id = ?", id)
		if err != nil {
			return nil, fmt.Errorf("deleting attr: %w", err)
		}
	}

	attrs2 := make([]*NodeAttribute, 0, len(attrs))
	for _, a := range attrs {
		if _, ok := existing[a.Name]; ok {
			continue
		}
		attrs2 = append(attrs2, a)
	}

	return attrs2, nil
}

func (e *executor) finaliseRelation(r ast.Relation, tx *sqlx.Tx) (*Relation, error) {
	now := time.Now().UTC()

	left, err := e.finaliseNode(r.Left(), tx)
	if err != nil {
		return nil, fmt.Errorf("finalising left node: %w", err)
	}

	right, err := e.finaliseNode(r.Right(), tx)
	if err != nil {
		return nil, fmt.Errorf("finalising right node: %w", err)
	}

	rel, err := e.findRelation(r, left.ID, right.ID, tx)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}

	if rel == nil {
		id, err := gonanoid.New()
		if err != nil {
			return nil, fmt.Errorf("rel id: %w", err)
		}
		rel = &Relation{
			ID:        id,
			CreatedAt: now,
		}
	} else {
		rel.UpdatedAt = &now
	}

	rel.Direction = r.Direction()
	rel.LeftNodeID = left.ID
	rel.RightNodeID = right.ID
	rel.leftNode = left
	rel.rightNode = right

	_, err = tx.NamedExec(`
		insert into relations(id, created_at, left_node_id, right_node_id, direction)
		values(:id, :created_at, :left_node_id, :right_node_id, :direction)
		on conflict(id) do update set
		updated_at = :updated_at,
		left_node_id = :left_node_id,
		right_node_id = :right_node_id,
		direction = :direction`, rel)
	if err != nil {
		return nil, fmt.Errorf("upserting relation: %w", err)
	}

	rel.labels, err = e.finaliseRelationLabels(rel.ID, r, tx)
	if err != nil {
		return nil, fmt.Errorf("finalising labels: %w", err)
	}

	rel.attributes, err = e.finaliseRelationAttributes(rel.ID, r, tx)
	if err != nil {
		return nil, fmt.Errorf("finalising attrs: %w", err)
	}

	return rel, nil
}

func (e *executor) finaliseRelationLabels(relationID string, r ast.Relation, tx *sqlx.Tx) ([]*RelationLabel, error) {
	now := time.Now().UTC()
	labels := []*RelationLabel{}

	if len(r.Labels()) == 0 {
		return labels, nil
	}

	err := tx.Select(&labels, "select * from relation_labels where relation_id = ?", relationID)
	if err != nil {
		return nil, fmt.Errorf("querying labels: %w", err)
	}

	existing := map[string]*RelationLabel{}
	for _, v := range labels {
		existing[v.Label] = v
	}

	for _, l := range r.Labels() {
		label := existing[l]
		if label == nil {
			id, err := gonanoid.New()
			if err != nil {
				return nil, fmt.Errorf("label id: %w", err)
			}
			label = &RelationLabel{
				ID:         id,
				CreatedAt:  now,
				RelationID: relationID,
				Label:      l,
			}
			labels = append(labels, label)
		} else {
			label.UpdatedAt = &now
		}

		_, err = tx.NamedExec(`
			insert into relation_labels(id, created_at, relation_id, label)
			values(:id, :created_at, :relation_id, :label)
			on conflict(id) do update set updated_at = :updated_at`, label)
		if err != nil {
			return nil, fmt.Errorf("inserting label: %w", err)
		}
		delete(existing, l)
	}

	for _, label := range existing {
		_, err = tx.Exec("delete from relation_labels where id = ?", label.ID)
		if err != nil {
			return nil, fmt.Errorf("deleting label: %w", err)
		}
	}

	labels2 := make([]*RelationLabel, 0, len(labels))
	for _, l := range labels {
		if _, ok := existing[l.Label]; ok {
			continue
		}
		labels2 = append(labels2, l)
	}

	return labels2, nil
}

func (e *executor) finaliseRelationAttributes(relationID string, r ast.Relation, tx *sqlx.Tx) ([]*RelationAttribute, error) {
	now := time.Now().UTC()
	attrs := []*RelationAttribute{}

	if len(r.Attributes()) == 0 {
		return attrs, nil
	}

	err := tx.Select(&attrs, "select * from relation_attributes where relation_id = ?", relationID)
	if err != nil {
		return nil, fmt.Errorf("querying attrs: %w", err)
	}

	existing := map[string]*RelationAttribute{}
	for _, a := range attrs {
		existing[a.Name] = a
	}

	for _, a := range r.Attributes() {
		attr := existing[a.Key()]
		if attr == nil {
			id, err := gonanoid.New()
			if err != nil {
				return nil, fmt.Errorf("attr id: %w", err)
			}
			attr = &RelationAttribute{
				ID:         id,
				CreatedAt:  now,
				RelationID: relationID,
				Name:       a.Key(),
			}
			attrs = append(attrs, attr)
		} else {
			attr.UpdatedAt = &now
		}
		attr.Value = a.Value()
		_, err = tx.NamedExec(`
			insert into relation_attributes(id, created_at, relation_id, attr_name, attr_value, data_type)
			values(:id, :created_at, :relation_id, :attr_name, :attr_value, :data_type)
			on conflict(id) do update set updated_at = :updated_at, attr_value = :attr_value`, &attr)
		if err != nil {
			return nil, fmt.Errorf("inserting attr: %w", err)
		}
		delete(existing, a.Key())
	}

	for _, id := range existing {
		_, err = tx.Exec("delete from relation_attributes where id = ?", id)
		if err != nil {
			return nil, fmt.Errorf("deleting attr: %w", err)
		}
	}

	attrs2 := make([]*RelationAttribute, 0, len(attrs))
	for _, a := range attrs {
		if _, ok := existing[a.Name]; ok {
			continue
		}
		attrs2 = append(attrs2, a)
	}

	return attrs2, nil
}

func (e *executor) finaliseMergeCmd(cmd ast.Command, tx *sqlx.Tx) (any, error) {
	switch cmd.Entity().Type() {
	case ast.EntityTypeNode:
		return e.finaliseNode(cmd.Entity(), tx)
	case ast.EntityTypeRelation:
		return e.finaliseRelation(cmd.Entity().(ast.Relation), tx)
	default:
		return nil, fmt.Errorf("unexpected entity: %v", cmd.Entity())
	}
}

func (e *executor) finaliseMatchCmd(cmd ast.Command, tx *sqlx.Tx) (SearchResults, error) {
	switch cmd.Entity().Type() {
	case ast.EntityTypeNode:
		node, err := e.findNode(cmd.Entity(), tx)
		if err != nil {
			return nil, err
		}
		ident := cmd.Entity().Identifier()
		if ident == "" {
			ident = "_"
		}
		return SearchResults{ident: node}, nil
	case ast.EntityTypeRelation:
		return e.searchNodes(cmd.Entity().(ast.Relation), cmd.Since(), tx)
	default:
		return nil, fmt.Errorf("unexpected entity: %v", cmd.Entity())
	}
}

func (e *executor) findNode(n ast.Entity, tx *sqlx.Tx) (*Node, error) {
	args := []any{}
	query := strings.Builder{}
	query.WriteString("select n.* from nodes n\n")

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
			args = append(args, v.Key())
			args = append(args, v.Value())
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

	res := &Node{}
	err := tx.Get(res, query.String(), args...)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("fetching node: %w", err)
		}
		return nil, ErrNotFound
	}

	res.attributes = []*NodeAttribute{}
	err = tx.Select(&res.attributes, "select * from node_attributes where node_id = ?", res.ID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("fetching node: %w", err)
		}
	}

	res.labels = []*NodeLabel{}
	err = tx.Select(&res.labels, "select * from node_labels where node_id = ?", res.ID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("fetching node: %w", err)
		}
	}

	return res, nil
}

func (e *executor) findRelation(r ast.Relation, leftNodeId, rightNodeId string, tx *sqlx.Tx) (*Relation, error) {
	args := []any{}
	query := strings.Builder{}
	query.WriteString("select r.* from relations r\n")

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
			args = append(args, v.Key())
			args = append(args, v.Value())
			i++
		}
		for _, l := range r.Labels() {
			query.WriteString(fmt.Sprintf(`
				inner join (select * from relation_labels where label = ?) rl%d
				on r.id = rl%d.relation_id`, i, i))
			args = append(args, l)
			i++
		}

		query.WriteString("\nwhere left_node_id = ? and right_node_id = ?")
		args = append(args, leftNodeId, rightNodeId)
	}

	// TODO: check only one matching row

	res := &Relation{}
	err := tx.Get(res, query.String(), args...)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("checking existing: %w", err)
		}
		return nil, ErrNotFound
	}
	res.attributes = []*RelationAttribute{}
	err = tx.Select(&res.attributes, "select * from relation_attributes where relation_id = ?", res.ID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("fetching relation: %w", err)
		}
	}

	res.labels = []*RelationLabel{}
	err = tx.Select(&res.labels, "select * from relation_labels where relation_id = ?", res.ID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("fetching relation: %w", err)
		}
	}

	return res, nil
}

func (e *executor) searchNodes(r ast.Relation, since time.Time, tx *sqlx.Tx) (SearchResults, error) {
	res := make(SearchResults)

	// left hand node must exist
	node, err := e.findNode(r.Left(), tx)
	if err != nil {
		return nil, fmt.Errorf("left node: %w", err)
	}

	ident := r.Left().Identifier()
	if ident != "" {
		res[ident] = node
	}

	rels, err := e.searchRelation(r, node.ID, "", tx)
	if err != nil {
		return nil, fmt.Errorf("left node: %w", err)
	}
	ident = r.Identifier()
	if ident != "" {
		res[ident] = rels
	}

	return res, nil
}

func (e *executor) searchRelation(r ast.Relation, leftNodeId, rightNodeId string, tx *sqlx.Tx) ([]*Relation, error) {
	results := []*Relation{}

	args := []any{}
	query := strings.Builder{}
	query.WriteString("select r.* from relations r\n")

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
			args = append(args, v.Key())
			args = append(args, v.Value())
			i++
		}
		for _, l := range r.Labels() {
			query.WriteString(fmt.Sprintf(`
				inner join (select * from relation_labels where label = ?) rl%d
				on r.id = rl%d.relation_id`, i, i))
			args = append(args, l)
			i++
		}

		ands := []string{}
		if leftNodeId != "" {
			ands = append(ands, "left_node_id = ?")
			args = append(args, leftNodeId)
		}
		if rightNodeId != "" {
			ands = append(ands, "right_node_id = ?")
			args = append(args, rightNodeId)
		}
		if len(ands) > 0 {
			query.WriteString("\nwhere ")
			query.WriteString(strings.Join(ands, " and "))
		}
	}

	rows, err := tx.Queryx(query.String(), args...)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("searching relations: %w", err)
		}
		return results, nil
	}
	defer rows.Close()
	for rows.Next() {
		r := &Relation{}
		err = rows.StructScan(r)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		r.attributes = []*RelationAttribute{}
		err = tx.Select(&r.attributes, "select * from relation_attributes where relation_id = ?", r.ID)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("fetching relation: %w", err)
			}
		}

		r.labels = []*RelationLabel{}
		err = tx.Select(&r.labels, "select * from relation_labels where relation_id = ?", r.ID)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("fetching relation: %w", err)
			}
		}

		results = append(results, r)
	}

	return results, nil
}
