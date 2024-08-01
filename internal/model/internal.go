package model

import (
	"time"
)

type HubSpec struct {
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
	HostAddr  string    `db:"host_addr"`
}

type PeerSpec struct {
	RemoteAddr string    `db:"remote_addr"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
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
