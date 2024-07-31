package peer

import (
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	rpc "github.com/jdudmesh/propolis/rpc/propolis/v1"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

type HandlerFunc func(msg *rpc.Envelope) error

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

type Connection struct {
	CreatedAt time.Time  `db:"created_at"`
	UpdatedAt *time.Time `db:"updated_at"`
	Type      PeerType   `db:"peer_type"`
	Handlers  map[string]HandlerFunc
	conn      quic.Connection
	stream    quic.Stream
	state     atomic.Int32
}

func New(t PeerType, cn quic.Connection, stm quic.Stream, state ConnectionStatus) *Connection {
	c := &Connection{
		CreatedAt: time.Now().UTC(),
		Type:      t,
		conn:      cn,
		stream:    stm,
		state:     atomic.Int32{},
	}
	c.SetState(state)
	return c
}

func (c *Connection) StreamID() quic.StreamID {
	return c.stream.StreamID()
}

func (c *Connection) HostAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *Connection) State() ConnectionStatus {
	return ConnectionStatus(c.state.Load())
}

func (c *Connection) SetState(s ConnectionStatus) {
	c.state.Store(int32(s))
}

func (c *Connection) Run() error {
	for {
		if c.stream.Context().Err() != nil {
			break
		}

		buf := bytes.NewBuffer(make([]byte, 0))

		for {
			b := make([]byte, 1024)
			n, err := c.stream.Read(b)
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

func (c *Connection) Close() error {
	return c.stream.Close()
}

func (c *Connection) Process(buf []byte) error {
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
	if handler, ok := c.Handlers[ct]; !ok {
		return errors.New("unknowm content type")
	} else {
		err := handler(e)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Connection) Dispatch(contentType string, payload protoreflect.ProtoMessage, inReplyToID string) error {
	var err error

	data := []byte{}
	if payload != nil {
		data, err = proto.Marshal(payload)
		if err != nil {
			return err
		}
	}

	id, err := gonanoid.New()
	if err != nil {
		return err
	}

	msg := &rpc.Envelope{
		Id:          id,
		InReplyTo:   inReplyToID,
		Timestamp:   time.Now().UTC().UnixNano(),
		Sender:      "xxx",
		ContentType: ContentTypePong,
		Payload:     data,
		Signature:   []byte{},
	}

	data, err = proto.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = c.stream.Write(data)
	if err != nil {
		return err
	}

	return nil
}
