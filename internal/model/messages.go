package model

import "time"

type ConnectionStatus int
type HostType int

const (
	ConnectionStatusNotConnected ConnectionStatus = iota
	ConnectionStatusConnecting
	ConnectionStatusDisconnecting
	ConnectionStatusConnected

	HostTypeHub HostType = iota
	HostTypeClient
)

type ClientConnection struct {
	Id          string           `db:"id"`
	CreatedAt   time.Time        `db:"created_at"`
	UpdatedAt   time.Time        `db:"updated_at"`
	Status      ConnectionStatus `db:"status"`
	HostType    HostType         `db:"host_type"`
	HostAddress string           `db:"host_addr"`
}
