package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"time"

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
	stm     quic.Stream
}

func (h *hubConnection) Connect(subscrtiptions []string) error {
	h.state.Store(stateConnecting)

	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"propolis"},
	}

	conn, err := quic.DialAddr(ctx, h.hubAddr, tlsConf, nil)
	if err != nil {
		h.state.Store(stateNotConnected)
		return err
	}

	defer conn.CloseWithError(0, "")

	h.stm, err = conn.OpenStreamSync(ctx)
	if err != nil {
		h.state.Store(stateNotConnected)
		return err
	}

	h.state.Store(stateConnected)
	return nil
}

func (h *hubConnection) Disconnect() error {
	h.state.Store(stateNotConnected)

	if h.stm == nil {
		return nil
	}

	err := h.stm.Close()
	if err != nil {
		return err
	}

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
