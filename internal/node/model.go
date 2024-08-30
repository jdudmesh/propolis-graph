package node

import (
	"github.com/jdudmesh/propolis/internal/graph"
	"github.com/jdudmesh/propolis/internal/identity"
)

const (
	MaxBodySize = 1048576

	HeaderRemoteAddress = "x-propolis-remote-address"
	HeaderActionID      = "x-propolis-action-id"
	HeaderNodeID        = "x-propolis-node-id"
	HeaderSender        = "x-propolis-sender"
	HeaderSignature     = "x-propolis-signature"
	HeaderIdentifier    = "x-propolis-identifier"
	HeaderReceivedBy    = "x-propolis-received-by"
	HeaderContentType   = "Content-Type"

	SelfRemoteAddress = "0.0.0.0"
	MaxPeers          = 3

	ContentTypeError     = "x-propolis/error"
	ContentTypePing      = "x-propolis/ping"
	ContentTypePong      = "x-propolis/pong"
	ContentTypeSubscribe = "x-propolis/subscribe"

	ContentTypeJSON = "application/json; utf-8"
)

type NodeType int

const (
	NodeTypeSeed NodeType = iota
	NodeTypePeer
	NodeTypeCache
)

type Config struct {
	graph.Config
	Host            string
	Port            int
	PublicAddress   string
	Seeds           []string
	NodeDatabaseURL string
	Type            NodeType
	Identity        identity.Identity
}

type Graph interface {
	Execute(action graph.Action) (any, error)
}
