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
	"time"

	"github.com/jdudmesh/propolis/internal/ast"
)

type Node struct {
	ID         string           `db:"id"`
	CreatedAt  time.Time        `db:"created_at"`
	UpdatedAt  *time.Time       `db:"updated_at"`
	labels     []*NodeLabel     `db:"-"`
	attributes []*NodeAttribute `db:"-"`
	Relations  []*Relation      `db:"-"`
}

type NodeAttribute struct {
	ID        string                `db:"id"`
	CreatedAt time.Time             `db:"created_at"`
	UpdatedAt *time.Time            `db:"updated_at"`
	NodeID    string                `db:"node_id"`
	Name      string                `db:"attr_name"`
	Value     string                `db:"attr_value"`
	Type      ast.AttributeDataType `db:"data_type"`
}

type NodeLabel struct {
	ID        string     `db:"id"`
	CreatedAt time.Time  `db:"created_at"`
	UpdatedAt *time.Time `db:"updated_at"`
	NodeID    string     `db:"node_id"`
	Label     string     `db:"label"`
}

type Relation struct {
	ID          string               `db:"id"`
	CreatedAt   time.Time            `db:"created_at"`
	UpdatedAt   *time.Time           `db:"updated_at"`
	LeftNodeID  string               `db:"left_node_id"`
	RightNodeID string               `db:"right_node_id"`
	Direction   ast.RelationDir      `db:"direction"`
	labels      []*RelationLabel     `db:"-"`
	attributes  []*RelationAttribute `db:"-"`
	leftNode    *Node                `db:"-"`
	rightNode   *Node                `db:"-"`
}

type RelationAttribute struct {
	ID         string                `db:"id"`
	CreatedAt  time.Time             `db:"created_at"`
	UpdatedAt  *time.Time            `db:"updated_at"`
	RelationID string                `db:"relation_id"`
	Name       string                `db:"attr_name"`
	Value      string                `db:"attr_value"`
	Type       ast.AttributeDataType `db:"data_type"`
}

type RelationLabel struct {
	ID         string     `db:"id"`
	CreatedAt  time.Time  `db:"created_at"`
	UpdatedAt  *time.Time `db:"updated_at"`
	RelationID string     `db:"relation_id"`
	Label      string     `db:"label"`
}

type SearchResults map[string]any
