package client

import (
	"context"
	"crypto/tls"

	"github.com/jdudmesh/propolis/internal/peer"
	rpc "github.com/jdudmesh/propolis/rpc/propolis/v1"
	"github.com/quic-go/quic-go"
)

const (
	statePermanentError int32 = -1
	stateNotConnected   int32 = 0
	stateConnecting     int32 = 1
	stateConnected      int32 = 2
)

type hubConnection struct {
	conn *peer.Connection
}

func Connect(ctx context.Context, addr string) (*hubConnection, error) {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"propolis"},
	}

	cn, err := quic.DialAddr(ctx, addr, tlsConf, nil)
	if err != nil {
		return nil, err
	}

	s, err := cn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	c := &hubConnection{
		conn: peer.New(peer.Hub, cn, s, peer.ConnectionStatusConnected),
	}

	return c, nil
}

func (h *hubConnection) Close() error {
	return h.conn.Close()
}

func (h *hubConnection) Subscribe(subs []string) error {
	msg := &rpc.Subscribe{
		Subscriptions: subs,
	}
	return h.conn.Dispatch(peer.ContentTypeSubscribe, msg, "")
}

func (h *hubConnection) Ping() error {
	return h.conn.Dispatch(peer.ContentTypePing, nil, "")
}
