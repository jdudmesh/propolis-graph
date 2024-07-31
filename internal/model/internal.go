package model

import (
	"time"

	"github.com/quic-go/quic-go"
)

type HubSpec struct {
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
	HostAddr  string    `db:"host_addr"`
}

type PeerSpec struct {
	CreatedAt time.Time     `db:"created_at"`
	UpdatedAt time.Time     `db:"updated_at"`
	StreamID  quic.StreamID `db:"stream_id"`
	HostAddr  string        `db:"host_addr"`
}

type SubscriptionSpec struct {
	PeerSpec
	ID           string `db:"id"`
	Subscription string `db:"spec"`
}

const (
	ContentTypeError     = "x-propolis/error"
	ContentTypePing      = "x-propolis/ping"
	ContentTypePong      = "x-propolis/pong"
	ContentTypeSubscribe = "x-propolis/subscribe"
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
