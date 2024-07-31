package hub

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/jdudmesh/propolis/internal/peer"
	rpc "github.com/jdudmesh/propolis/rpc/propolis/v1"
	"github.com/quic-go/quic-go"
)

const defaultHubPort = 9000

type internalStateStore interface {
	UpsertHub(h peer.HubSpec) error
	GetHubs() ([]*peer.HubSpec, error)
	UpsertPeer(p peer.PeerSpec) error
	UpsertSubscription(s peer.SubscriptionSpec) error
	FindPeersBySubscription(s string) ([]*peer.PeerSpec, error)
}

type worker struct {
	hostAddr    string
	store       internalStateStore
	quit        chan struct{}
	connections map[quic.StreamID]*clientConnection
}

func New(configHost string, configPort int, store internalStateStore) (*worker, error) {
	if configHost == "" {
		configHost = "localhost"
	}
	if configPort == 0 {
		configPort = defaultHubPort
	}

	return &worker{
		hostAddr:    fmt.Sprintf("%s:%d", configHost, configPort),
		store:       store,
		quit:        make(chan struct{}),
		connections: make(map[quic.StreamID]*clientConnection),
	}, nil
}

func (w *worker) Run() error {
	listener, err := quic.ListenAddr(w.hostAddr, generateTLSConfig(), nil)
	if err != nil {
		return err
	}
	defer listener.Close()

	upsertPeer := make(chan peer.PeerSpec)
	defer close(upsertPeer)

	upsertSubs := make(chan peer.SubscriptionSpec)
	defer close(upsertSubs)

	go func() {
		for {
			select {
			case p := <-upsertPeer:
				err := w.store.UpsertPeer(p)
				if err != nil {
					slog.Error("refreshing peer", "error", err)
				}
			case s := <-upsertSubs:
				err := w.store.UpsertSubscription(s)
				if err != nil {
					slog.Error("refreshing subscription", "error", err)
				}
				err = w.broadcastSubscriptions(s)
				if err != nil {
					slog.Error("broadcastings subscription", "error", err)
				}
			}
		}
	}()

	fmt.Println("Waiting for connections on: " + w.hostAddr)
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			return err
		}

		client, err := Accept(ctx, conn, upsertPeer, upsertSubs)
		if err != nil {
			return err
		}

		w.connections[client.conn.StreamID()] = client

		go client.Run()
	}
}

func (w *worker) Close() error {
	for _, cn := range w.connections {
		cn.Close()
	}
	close(w.quit)
	return nil
}

func (w *worker) broadcastSubscriptions(s peer.SubscriptionSpec) error {
	hubs, err := w.store.GetHubs()
	if err != nil {
		return err
	}

	hubsList := make([]string, 0, len(hubs))
	for _, h := range hubs {
		hubsList = append(hubsList, h.HostAddr)
	}

	peers, err := w.store.FindPeersBySubscription(s.Subscription)
	if err != nil {
		return err
	}

	peerList := make([]string, 0, len(peers))
	for _, p := range peers {
		peerList = append(peerList, p.HostAddr)
	}

	msg := &rpc.SubscriptionUpdate{
		Hubs:  hubsList,
		Peers: peerList,
	}

	for _, p := range peers {
		if cn, ok := w.connections[p.StreamID]; ok {
			err := cn.conn.Dispatch(peer.ContentTypeSubscribe, msg, "")
			if err != nil {
				slog.Error("sending subs update", "error", err, "StreamID", cn.conn.StreamID())
			}
		}
	}

	return nil
}

func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"propolis"},
	}
}
