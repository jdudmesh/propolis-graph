package node

import (
	"github.com/jdudmesh/propolis/internal/graph"
	"github.com/jdudmesh/propolis/internal/identity"
)

const (
	ContentTypeHeader = "Content-Type"

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
