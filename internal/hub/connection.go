package hub

import (
	"context"
	"log/slog"
	"time"

	"github.com/jdudmesh/propolis/internal/peer"
	rpc "github.com/jdudmesh/propolis/rpc/propolis/v1"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

type clientConnection struct {
	conn       *peer.Connection
	upsertPeer chan peer.PeerSpec
	upsertSubs chan peer.SubscriptionSpec
}

func Accept(
	ctx context.Context,
	cn quic.Connection,
	upsertPeer chan peer.PeerSpec,
	upsertSubs chan peer.SubscriptionSpec) (*clientConnection, error) {

	s, err := cn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}

	c := &clientConnection{
		conn:       peer.New(peer.Client, cn, s, peer.ConnectionStatusConnected),
		upsertPeer: upsertPeer,
		upsertSubs: upsertSubs,
	}

	c.conn.Handlers = map[string]peer.HandlerFunc{
		peer.ContentTypePing:      c.handlePing,
		peer.ContentTypeSubscribe: c.handleSubscribe,
	}

	slog.Info("new client", "addr", c.conn.HostAddr())

	return c, nil
}

func (c *clientConnection) Close() error {
	return c.conn.Close()
}

func (c *clientConnection) Run() error {
	c.upsertPeer <- peer.PeerSpec{
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		StreamID:  c.conn.StreamID(),
		HostAddr:  c.conn.HostAddr(),
	}
	return c.conn.Run()
}

func (c *clientConnection) refreshSubscription(s string) error {
	c.upsertSubs <- peer.SubscriptionSpec{
		PeerSpec: peer.PeerSpec{
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
			StreamID:  c.conn.StreamID(),
			HostAddr:  c.conn.HostAddr(),
		},
		Subscription: s,
	}
	return nil
}

func (c *clientConnection) handlePing(e *rpc.Envelope) error {
	return c.conn.Dispatch(peer.ContentTypePong, nil, e.Id)
}

func (c *clientConnection) handleSubscribe(e *rpc.Envelope) error {
	req := &rpc.Subscribe{}
	err := proto.Unmarshal(e.Payload, req)
	if err != nil {
		return err
	}

	slog.Info("subscription request", "req", req)

	for _, s := range req.Subscriptions {
		err = c.refreshSubscription(s)
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
