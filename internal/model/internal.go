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
	"errors"
	"time"
)

var ErrAlreadyExists = errors.New("entity already exists")
var ErrNotFound = errors.New("entity not found")

type HubSpec struct {
	CreatedAt time.Time  `db:"created_at"`
	UpdatedAt *time.Time `db:"updated_at"`
	HostAddr  string     `db:"host_addr"`
}

type PeerSpec struct {
	RemoteAddr string     `db:"remote_addr"`
	CreatedAt  time.Time  `db:"created_at"`
	UpdatedAt  *time.Time `db:"updated_at"`
}

type SubscriptionSpec struct {
	PeerSpec
	Spec string `db:"spec"`
}

const (
	ContentTypeHeader = "Content-Type"

	ContentTypeError     = "x-propolis/error"
	ContentTypePing      = "x-propolis/ping"
	ContentTypePong      = "x-propolis/pong"
	ContentTypeSubscribe = "x-propolis/subscribe"

	ContentTypeJSON = "application/json; utf-8"
)

type ConnectionStatus int
type PeerType int

const (
	ConnectionStatusNotConnected ConnectionStatus = iota
	ConnectionStatusConnecting
	ConnectionStatusDisconnecting
	ConnectionStatusConnected

	Hub PeerType = iota
	Client
)
