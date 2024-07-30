package hub

import (
	"bytes"
	"errors"
	"strings"
	"time"

	"github.com/jdudmesh/propolis/internal/model"
	rpc "github.com/jdudmesh/propolis/rpc/propolis/v1"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

type handlerFunc func(c *clientConnection, payload []byte) error

const (
	ContentTypeError     = "x-propolis/error"
	ContentTypePing      = "x-propolis/ping"
	ContentTypePong      = "x-propolis/pong"
	ContentTypeSubscribe = "x-propolis/subscribe"
)

var handlers = map[string]handlerFunc{
	ContentTypeError:     func(c *clientConnection, payload []byte) error { return nil },
	ContentTypePing:      func(c *clientConnection, payload []byte) error { return nil },
	ContentTypePong:      func(c *clientConnection, payload []byte) error { return nil },
	ContentTypeSubscribe: func(c *clientConnection, payload []byte) error { return nil },
}

type clientConnection struct {
	model.ClientConnection
	stm quic.Stream
}

func NewClientConn(stm quic.Stream) (*clientConnection, error) {
	id, err := gonanoid.New()
	if err != nil {
		return nil, err
	}

	return &clientConnection{
		model.ClientConnection{
			Id:        id,
			CreatedAt: time.Now().UTC(),
			Status:    model.ConnectionStatusConnected,
			HostType:  model.HostTypeClient,
		},
		stm,
	}, nil
}

func (c *clientConnection) Run() error {
	for {
		if c.stm.Context().Err() != nil {
			break
		}

		buf := bytes.NewBuffer(make([]byte, 0))

		for {
			b := make([]byte, 1024)
			n, err := c.stm.Read(b)
			if err != nil {
				return err
			}
			buf.Write(b[:n])
			if n < len(b) {
				break
			}
		}

		c.Process(buf.Bytes())
	}

	return nil
}

func (c *clientConnection) Close() error {
	return c.stm.Close()
}

func (c *clientConnection) Process(buf []byte) error {
	e := &rpc.Envelope{}
	err := proto.Unmarshal(buf, e)
	if err != nil {
		return err
	}

	f := strings.Split(e.ContentType, ";")
	if len(f) == 0 {
		return errors.New("bad content type")
	}
	ct := strings.ToLower(strings.TrimSpace(f[0]))
	if handler, ok := handlers[ct]; !ok {
		return errors.New("unknowm content type")
	} else {
		err := handler(c, e.Payload)
		if err != nil {
			return err
		}
	}

	return nil
}

func handleError(c *clientConnection, payload []byte) error {
	return nil
}

func handlePing(c *clientConnection, payload []byte) error {
	return nil
}

func handlePong(c *clientConnection, payload []byte) error {
	return nil
}

func handleSubscribe(c *clientConnection, payload []byte) error {
	e := &rpc.SubscribeRequest{}
	err := proto.Unmarshal(payload, e)
	if err != nil {
		return err
	}

	return nil
}
