package executor

import (
	"time"

	"github.com/jdudmesh/propolis/internal/ast"
)

type Node struct {
	ID         ast.EntityID     `db:"id"`
	CreatedAt  time.Time        `db:"created_at"`
	UpdatedAt  *time.Time       `db:"updated_at"`
	labels     []*NodeLabel     `db:"-"`
	attributes []*NodeAttribute `db:"-"`
}

type NodeAttribute struct {
	ID        ast.EntityID          `db:"id"`
	CreatedAt time.Time             `db:"created_at"`
	UpdatedAt *time.Time            `db:"updated_at"`
	NodeID    ast.EntityID          `db:"node_id"`
	Name      string                `db:"attr_name"`
	Value     string                `db:"attr_value"`
	Type      ast.AttributeDataType `db:"data_type"`
}

type NodeLabel struct {
	ID        ast.EntityID `db:"id"`
	CreatedAt time.Time    `db:"created_at"`
	UpdatedAt *time.Time   `db:"updated_at"`
	NodeID    ast.EntityID `db:"node_id"`
	Label     string       `db:"label"`
}

type Relation struct {
	ID          ast.EntityID         `db:"id"`
	CreatedAt   time.Time            `db:"created_at"`
	UpdatedAt   *time.Time           `db:"updated_at"`
	LeftNodeID  ast.EntityID         `db:"left_node_id"`
	RightNodeID ast.EntityID         `db:"right_node_id"`
	Direction   ast.RelationDir      `db:"direction"`
	labels      []*RelationLabel     `db:"-"`
	attributes  []*RelationAttribute `db:"-"`
	leftNode    *Node                `db:"-"`
	rightNode   *Node                `db:"-"`
}

type RelationAttribute struct {
	ID         ast.EntityID          `db:"id"`
	CreatedAt  time.Time             `db:"created_at"`
	UpdatedAt  *time.Time            `db:"updated_at"`
	RelationID ast.EntityID          `db:"relation_id"`
	Name       string                `db:"attr_name"`
	Value      string                `db:"attr_value"`
	Type       ast.AttributeDataType `db:"data_type"`
}

type RelationLabel struct {
	ID         ast.EntityID `db:"id"`
	CreatedAt  time.Time    `db:"created_at"`
	UpdatedAt  *time.Time   `db:"updated_at"`
	RelationID ast.EntityID `db:"relation_id"`
	Label      string       `db:"label"`
}

// func (n *Node) ID() ast.EntityID {
// 	return n.ID
// }

// func (n *Node) WithID(EntityID) ast.Entity {
// 	return nil
// }

// func (n *Node) Type() ast.EntityType {
// 	return n.Type
// }

// func (n *Node) Identifier() string {
// 	return n.Identifier
// }

// func (n *Node) Labels() []string {
// 	labels := make([]string, len(n.labels))
// 	for i, x := range n.labels {
// 		labels[i] = x
// 	}
// 	return labels
// }

// func (n *Node) Attributes() map[string]ast.Attribute {
// 	attrs := make(map[string]ast.Attribute)
// 	for _, v := range n.attributes {
// 		attrs[v.Name] = v
// 	}
// 	return attrs
// }

// func (n *Node) Attribute(key string) (any, bool) {
// 	for _, v := range n.attributes {
// 		if v.Name == key {
// 			return v.Value, true
// 		}
// 	}
// 	return nil, false
// }
