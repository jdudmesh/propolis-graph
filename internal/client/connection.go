package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jdudmesh/propolis/internal/hub"
	rpc "github.com/jdudmesh/propolis/rpc/propolis/v1"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

const (
	statePermanentError int32 = -1
	stateNotConnected   int32 = 0
	stateConnecting     int32 = 1
	stateConnected      int32 = 2
)

type hubConnection struct {
	hubAddr string
	state   atomic.Int32
	conn    quic.Connection
	stm     quic.Stream
}

func (h *hubConnection) Connect(subscrtiptions []string) error {
	var err error
	h.state.Store(stateConnecting)

	ctx, cancelFn := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancelFn()

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"propolis"},
	}

	h.conn, err = quic.DialAddr(ctx, h.hubAddr, tlsConf, nil)
	if err != nil {
		h.state.Store(stateNotConnected)
		return err
	}

	h.stm, err = h.conn.OpenStreamSync(ctx)
	if err != nil {
		h.state.Store(stateNotConnected)
		return err
	}

	h.state.Store(stateConnected)
	return nil
}

func (h *hubConnection) Disconnect() error {
	h.state.Store(stateNotConnected)

	if h.conn != nil {
		return nil
	}
	h.conn.CloseWithError(0, "normal shutdown")
	h.conn = nil

	if h.stm == nil {
		return nil
	}

	err := h.stm.Close()
	if err != nil {
		return err
	}

	h.stm = nil

	return nil
}

func (h *hubConnection) Ping() error {
	m := &rpc.Ping{
		Timestamp: time.Now().UTC().UnixNano(),
	}

	payload, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshalling message: %w", err)
	}

	e := &rpc.Envelope{
		Payload: payload,
	}

	r, err := proto.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshalling message: %w", err)
	}

	_, err = h.stm.Write(r)
	if err != nil {
		return err
	}

	return nil
}

func (h *hubConnection) SendSubscriptions(subs []string) error {
	if h.stm == nil {
		return nil
	}

	m := &rpc.Subscribe{
		Subscriptions: subs,
	}

	payload, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshalling message: %w", err)
	}

	e := &rpc.Envelope{
		ContentType: hub.ContentTypeSubscribe,
		Payload:     payload,
	}

	r, err := proto.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshalling message: %w", err)
	}

	_, err = h.stm.Write(r)
	if err != nil {
		return err
	}

	return nil
}
