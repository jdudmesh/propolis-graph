package peer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"time"

	"github.com/jdudmesh/propolis/internal/model"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type peerStore interface {
	GetPeers() ([]*model.PeerSpec, error)
	GetSubs() ([]*model.SubscriptionSpec, error)
	UpsertSubs(remoteAddr string, subs []string) error
	FindPeersBySub(sub string) ([]*model.PeerSpec, error)
}

type peer struct {
	nodeID string
	host   string
	port   int
	store  peerStore
	server *http3.Server
	client *http.Client
	quit   chan struct{}
}

func New(host string, port int, store peerStore) (*peer, error) {
	nodeID, err := gonanoid.New()
	if err != nil {
		return nil, fmt.Errorf("generating node id: %w", err)
	}

	p := &peer{
		nodeID: nodeID,
		host:   host,
		port:   port,
		store:  store,
		quit:   make(chan struct{}),
	}

	p.server = &http3.Server{
		Handler: p.newServeMux(),
	}

	return p, nil
}

func (p *peer) newServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscription", p.handleCreateSubscription)
	mux.HandleFunc("DELETE /subscription", p.handleDeleteSubscription)
	mux.HandleFunc("POST /action", p.handleCreateAction)
	mux.HandleFunc("POST /ping", p.handlePing)
	mux.HandleFunc("POST /pong", p.handlePong)
	return mux
}

func (p *peer) Run() error {
	defer p.server.CloseGracefully(10 * time.Second)

	addr := &net.UDPAddr{IP: net.ParseIP(p.host), Port: p.port}
	slog.Info("starting listener", "addr", addr)

	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("creating sock: %w", err)
	}

	tr := quic.Transport{
		Conn: udpConn,
	}
	defer tr.Close()

	roundTripper := &http3.RoundTripper{
		TLSClientConfig: &tls.Config{
			NextProtos:         []string{"h3", "propolis"},
			InsecureSkipVerify: true,
		},
		QUICConfig: &quic.Config{}, // QUIC connection options
		Dial: func(ctx context.Context, addr string, tlsConf *tls.Config, quicConf *quic.Config) (quic.EarlyConnection, error) {
			slog.Debug("dialing", "addr", addr)
			a, err := net.ResolveUDPAddr("udp", addr)
			if err != nil {
				return nil, err
			}
			return tr.DialEarly(ctx, a, tlsConf, quicConf)
		},
	}
	defer roundTripper.Close()

	p.client = &http.Client{
		Transport: roundTripper,
	}

	listener, err := tr.ListenEarly(p.generateTLSConfig(), nil)
	if err != nil {
		return fmt.Errorf("setting up listener sock: %w", err)
	}

	go func() {
		err := p.server.ServeListener(listener)
		if err != nil {
			slog.Error("closing peer server", "error", err)
		}
	}()

	err = p.refreshSubscriptions()
	if err != nil {
		return fmt.Errorf("refreshing subscriptions: %w", err)
	}

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err := p.refreshSubscriptions()
			if err != nil {
				slog.Error("refreshing subscriptions", "error", err)
			}
			roundTripper.CloseIdleConnections()
		case <-p.quit:
			return nil
		}
	}
}

func (p *peer) Close() error {
	close(p.quit)
	return nil
}

func (p *peer) handleCreateSubscription(w http.ResponseWriter, req *http.Request) {
	params := model.SubscriptionRequest{}

	body := req.Body
	defer body.Close()

	dec := json.NewDecoder(body)
	err := dec.Decode(&params)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = p.store.UpsertSubs(req.RemoteAddr, params.Spec)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp := &model.SubscriptionResponse{
		Peers: make(map[string][]string),
	}

	for _, s := range params.Spec {
		peers, err := p.store.FindPeersBySub(s)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		resp.Peers[s] = make([]string, len(peers))
		for i, peer := range peers {
			resp.Peers[s][i] = peer.RemoteAddr
		}
	}

	data, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(data)
	w.WriteHeader(http.StatusCreated)
}

func (p *peer) handleDeleteSubscription(w http.ResponseWriter, req *http.Request) {
}

func (p *peer) handleCreateAction(w http.ResponseWriter, req *http.Request) {
}

func (p *peer) handlePing(w http.ResponseWriter, req *http.Request) {
	slog.Info("ping", "remote", req.RemoteAddr)

	resp := model.PingResponse{
		Address: req.RemoteAddr,
	}

	data, err := json.Marshal(&resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Add(model.ContentTypeHeader, model.ContentTypeJSON)
	w.Write(data)
}

func (p *peer) sendPong(addr string) {
	resp, err := p.client.Post(fmt.Sprintf("https://%s/pong", addr), model.ContentTypeJSON, nil)
	if err != nil {
		slog.Error("sending pong", "error", err, "remote", addr)
	}

	if resp.StatusCode != http.StatusOK {
		slog.Error("bad pong response", "remote", addr)
	}
}

func (p *peer) handlePong(w http.ResponseWriter, req *http.Request) {
	slog.Info("pong", "remote", req.RemoteAddr)
	w.WriteHeader(http.StatusOK)
}

func (p *peer) pingPeers() error {
	peers, err := p.store.GetPeers()
	if err != nil {
		return fmt.Errorf("fetching peers: %w", err)
	}
	for _, peer := range peers {
		resp, err := p.client.Post(fmt.Sprintf("https://%s/ping", peer), model.ContentTypeJSON, nil)
		if err != nil {
			slog.Error("sending ping", "error", err, "remote", peer)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			slog.Error("bad pong response", "remote", peer)
		}
	}

	return nil
}

func (p *peer) refreshSubscriptions() error {
	peers, err := p.store.GetPeers()
	if err != nil {
		return fmt.Errorf("fetching peers: %w", err)
	}

	subs, err := p.store.GetSubs()
	if err != nil {
		return fmt.Errorf("fetching subs: %w", err)
	}
	specs := make([]string, len(subs))
	for i, s := range subs {
		specs[i] = s.Spec
	}

	for _, peer := range peers {
		err = p.sendSub(peer.RemoteAddr, specs)
		if err != nil {
			slog.Error("refreshing sub", "error", err, "peer", peer, "subs", subs)
			continue
		}
	}

	return nil
}

func (p *peer) sendSub(peer string, subs []string) error {
	params := model.SubscriptionRequest{
		Spec: subs,
	}

	data, err := json.Marshal(&params)
	if err != nil {
		return fmt.Errorf("marshalling subscription req: %w", err)
	}

	buf := bytes.NewBuffer(data)
	resp, err := p.client.Post(fmt.Sprintf("https://%s/subscription", peer), model.ContentTypeJSON, buf)
	if err != nil {
		return fmt.Errorf("sending subscription req: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to create subscription")
	}

	return nil
}

func (p *peer) generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{
		Subject: pkix.Name{
			CommonName: p.nodeID,
		},
		SerialNumber: big.NewInt(1),
	}
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
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{tlsCert},
		NextProtos:         []string{"h3", "propolis"},
	}
}
