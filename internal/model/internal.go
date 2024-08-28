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
package model

import (
	"crypto/rand"
	"errors"
	"math/big"
	"time"

	"github.com/bwmarrin/snowflake"
)

const PROPOLIS_NODE_ID = "PROPOLIS_NODE_ID"

var snowflakeNode *snowflake.Node

func init() {
	seed, err := rand.Int(rand.Reader, big.NewInt(1023))
	if err != nil {
		panic(err)
	}
	node, err := snowflake.NewNode(seed.Int64())
	if err != nil {
		panic(err)
	}
	snowflakeNode = node
}

func NewID() string {
	id := snowflakeNode.Generate()
	return id.Base58()
}

var ErrAlreadyExists = errors.New("entity already exists")
var ErrNotFound = errors.New("entity not found")
var ErrNotAcceptable = errors.New("entity not acceptable")

type SeedSpec struct {
	CreatedAt  time.Time  `db:"created_at"`
	UpdatedAt  *time.Time `db:"updated_at"`
	RemoteAddr string     `db:"remote_addr"`
	NodeID     string     `db:"node_id"`
}

type PeerSpec struct {
	RemoteAddr string     `db:"remote_addr"`
	CreatedAt  time.Time  `db:"created_at"`
	UpdatedAt  *time.Time `db:"updated_at"`
	NodeID     string     `db:"node_id"`
	Filter     string     `db:"filter" json:"filter,omitempty"`
}

type SubscriptionSpec struct {
	PeerSpec
	Spec string `db:"spec"`
}
