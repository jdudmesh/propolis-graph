package hub

import (
	"context"

	"github.com/jdudmesh/propolis/internal/peer"
	rpc "github.com/jdudmesh/propolis/rpc/propolis/v1"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

type clientConnection struct {
	conn *peer.Connection
}

func Accept(ctx context.Context, cn quic.Connection) (*clientConnection, error) {
	s, err := cn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}

	c := &clientConnection{
		conn: peer.New(peer.Client, cn, s, peer.ConnectionStatusConnected),
	}

	c.conn.Handlers = map[string]peer.HandlerFunc{
		peer.ContentTypePing: c.handlePing,
	}

	return c, nil
}

func (c *clientConnection) Close() error {
	return c.conn.Close()
}

func (c *clientConnection) Connection() *peer.Connection {
	return c.conn
}

func (c *clientConnection) RefreshSubscription(subscription string) error {
	return nil
}

func (c *clientConnection) handlePing(e *rpc.Envelope) error {
	return c.conn.Dispatch(peer.ContentTypePong, nil, e.Id)
}

func (c *clientConnection) handleSubscribe(e *rpc.Envelope) error {
	req := &rpc.SubscribeRequest{}
	err := proto.Unmarshal(e.Payload, req)
	if err != nil {
		return err
	}

	for _, s := range req.Subscriptions {
		err = c.RefreshSubscription(s)
		if err != nil {
			errMsg := &rpc.Error{Message: "unable to refresh subscription"}
			err2 := c.conn.Dispatch(peer.ContentTypeError, errMsg, e.Id)
			if err2 != nil {
				return err2
			}
			return err
		}
	}

	return nil
}
