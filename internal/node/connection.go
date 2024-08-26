/*
Copyright © 2024 John Dudmesh <john@dudmesh.co.uk>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/
package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jdudmesh/propolis/internal/ast"
	"github.com/jdudmesh/propolis/internal/executor"
	"github.com/jdudmesh/propolis/internal/model"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

const (
	MaxBodySize = 1048576

	HeaderRemotAddress = "x-propolis-remote-address"
	HeaderActionID     = "x-propolis-action-id"
	HeaderNodeID       = "x-propolis-node-id"
	HeaderSender       = "x-propolis-sender"
	HeaderSignature    = "x-propolis-signature"
	HeaderIdentifier   = "x-propolis-identifier"

	SelfRemoteAddress = "0.0.0.0"
)

type NodeType int

const (
	NodeTypeSeed NodeType = iota
	NodeTypePeer
	NodeTypeCache
)

type Action struct {
	ID         string
	RemoteAddr string
	NodeID     string
	Timestamp  time.Time
	Action     string
	Command    ast.Command
}

type peerStore interface {
	executor.ExecutorStore
	GetSeeds() ([]*model.PeerSpec, error)
	UpsertSeeds([]string) error
	GetPeers() ([]*model.PeerSpec, error)
	TouchPeer(remoteAddr string) error
	UpsertPeersForSub(sub string, peers []string) error
	AddPendingPeer(remoteAddr string, sub string) error
	RemovePendingPeer(remoteAddr string, sub string) error
	GetPendingPeersForSub(sub string) ([]*model.SubscriptionSpec, error)
	GetSelfSubs() ([]*model.SubscriptionSpec, error)
	UpsertSubs(remoteAddr string, subs []string) error
	DeleteSubs(remoteAddr string, subs []string) error
	FindPeersBySub(sub string) ([]*model.PeerSpec, error)
	AddAction(id, action, remoteAddr string) error
	GetCertificate(identifier string) (x509.Certificate, error)
}

type peer struct {
	nodeID             string
	host               string
	port               int
	store              peerStore
	logger             *slog.Logger
	roundTripper       *http3.RoundTripper
	server             *http3.Server
	client             *http.Client
	notifyPendingPeers chan string
	actionQueue        chan Action
	quit               chan struct{}
	remoteAddr         string
	nodeType           NodeType
}

func NewSeed(host string, port int, store peerStore, logger *slog.Logger) (*peer, error) {
	return new(host, port, store, logger, NodeTypeSeed)
}

func NewPeer(host string, port int, store peerStore, logger *slog.Logger) (*peer, error) {
	return new(host, port, store, logger, NodeTypePeer)
}

func NewCache(host string, port int, store peerStore, logger *slog.Logger) (*peer, error) {
	return new(host, port, store, logger, NodeTypeCache)
}

func new(host string, port int, store peerStore, logger *slog.Logger, nodeType NodeType) (*peer, error) {
	nodeID, err := gonanoid.New()
	if err != nil {
		return nil, fmt.Errorf("generating node id: %w", err)
	}

	p := &peer{
		nodeID:             nodeID,
		host:               host,
		port:               port,
		store:              store,
		logger:             logger,
		nodeType:           nodeType,
		notifyPendingPeers: make(chan string),
		actionQueue:        make(chan Action),
		quit:               make(chan struct{}),
	}

	p.server = &http3.Server{
		Handler: p.newServeMux(),
	}

	return p, nil
}

func (p *peer) newServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	switch p.nodeType {
	case NodeTypeSeed:
		fallthrough
	case NodeTypePeer:
		mux.HandleFunc("POST /subscription", p.handleCreateSubscription)
		mux.HandleFunc("DELETE /subscription", p.handleDeleteSubscription)
		mux.HandleFunc("POST /subscription/peer", p.handleSubscriptionPeerUpdate)
		mux.HandleFunc("POST /ping", p.handlePing)
		mux.HandleFunc("POST /pong", p.handlePong)
	case NodeTypeCache:
		mux.HandleFunc("POST /action", p.handleCreateAction)
	}
	return mux
}

func (p *peer) Run() error {
	defer p.server.CloseGracefully(10 * time.Second)

	addr := &net.UDPAddr{IP: net.ParseIP(p.host), Port: p.port}
	switch p.nodeType {
	case NodeTypePeer:
		p.logger.Info("starting peer", "addr", addr)
	case NodeTypeSeed:
		p.logger.Info("starting seed", "addr", addr)
	}

	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("creating sock: %w", err)
	}

	tr := quic.Transport{
		Conn: udpConn,
	}
	defer tr.Close()

	p.roundTripper = &http3.RoundTripper{
		TLSClientConfig: &tls.Config{
			NextProtos:         []string{"h3", "propolis"},
			InsecureSkipVerify: true,
		},
		QUICConfig: &quic.Config{}, // QUIC connection options
		Dial: func(ctx context.Context, addr string, tlsConf *tls.Config, quicConf *quic.Config) (quic.EarlyConnection, error) {
			p.logger.Debug("dialing", "addr", addr)
			a, err := net.ResolveUDPAddr("udp", addr)
			if err != nil {
				return nil, err
			}
			return tr.DialEarly(ctx, a, tlsConf, quicConf)
		},
	}
	defer p.roundTripper.Close()

	p.client = &http.Client{
		Transport: p.roundTripper,
	}

	listener, err := tr.ListenEarly(p.generateTLSConfig(), nil)
	if err != nil {
		return fmt.Errorf("setting up listener sock: %w", err)
	}

	go func() {
		err := p.server.ServeListener(listener)
		if err != nil {
			p.logger.Error("closing peer server", "error", err)
		}
	}()

	err = p.pingSeeds()
	if err != nil {
		return fmt.Errorf("refreshing seeds: %w", err)
	}

	switch p.nodeType {
	case NodeTypePeer:
		return p.runLoopPeer()
	case NodeTypeSeed:
		return p.runLoopSeed()
	case NodeTypeCache:
		return p.runLoopCache()
	}

	return nil
}

func (p *peer) runLoopPeer() error {
	err := p.getInitialPeers()
	if err != nil {
		return fmt.Errorf("obtaining peers: %w", err)
	}

	t1 := time.NewTicker(5 * time.Second)
	defer t1.Stop()
	t2 := time.NewTicker(60 * time.Minute)
	defer t2.Stop()

	for {
		select {
		case <-t1.C:
			err := p.refreshSubs()
			if err != nil {
				p.logger.Error("refreshing subscriptions", "error", err)
			}
			p.roundTripper.CloseIdleConnections()
		case <-t2.C:
			err := p.pingSeeds()
			if err != nil {
				p.logger.Error("refreshing seeds", "error", err)
			}
		case <-p.quit:
			return nil
		}
	}
}

func (p *peer) runLoopSeed() error {
	t1 := time.NewTicker(5 * time.Second)
	defer t1.Stop()
	t2 := time.NewTicker(60 * time.Minute)
	defer t2.Stop()

	for {
		select {
		case sub := <-p.notifyPendingPeers:
			err := p.doNotifyPendingPeers(sub)
			if err != nil {
				p.logger.Error("notifying peers", "error", err)
			}
		case <-t1.C:
			p.roundTripper.CloseIdleConnections()
		case <-t2.C:
			err := p.pingSeeds()
			if err != nil {
				p.logger.Error("refreshing seeds", "error", err)
			}
		case <-p.quit:
			return nil
		}
	}
}

func (p *peer) runLoopCache() error {
	dispatchQueue := make(chan any)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for a := range dispatchQueue {
			fmt.Println(a)
		}
	}()

outer:
	for {
		select {
		case action := <-p.actionQueue:
			e := executor.New(action.Command, p.store, p.logger)
			res, err := e.Execute()
			if err != nil {
				p.logger.Error("executing action", "error", err)
				continue
			}
			dispatchQueue <- res
		case <-p.quit:
			break outer
		}
	}

	close(dispatchQueue)
	wg.Wait()

	return nil
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

	p.logger.Debug("upsert sub for peer", "sub", params.Spec, "peer", req.RemoteAddr)
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

		// check if this is the only peer
		if len(resp.Peers[s]) == 1 {
			p.logger.Debug("adding pending peer", "remoteAddr", req.RemoteAddr, "sub", s)
			p.store.AddPendingPeer(req.RemoteAddr, s)
		} else {
			p.notifyPendingPeers <- s
		}
	}

	data, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Add(HeaderRemotAddress, req.RemoteAddr)
	w.WriteHeader(http.StatusCreated)
	w.Write(data)
}

func (p *peer) doNotifyPendingPeers(sub string) error {
	pendingPeers, err := p.store.GetPendingPeersForSub(sub)
	if err != nil {
		return fmt.Errorf("fetching pending peers: %w", err)
	}

	if len(pendingPeers) == 0 {
		return nil
	}
	p.logger.Debug("notifying pending peers", "n", len(pendingPeers))

	notification := &model.SubscriptionResponse{
		Peers: make(map[string][]string),
	}

	peers, err := p.store.FindPeersBySub(sub)
	if err != nil {
		return fmt.Errorf("finding peers: %w", err)
	}

	notification.Peers[sub] = make([]string, len(peers))
	for i, peer := range peers {
		notification.Peers[sub][i] = peer.RemoteAddr
	}

	data, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("marshalling response: %w", err)
	}

	wg := sync.WaitGroup{}
	for _, pp := range pendingPeers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			p.logger.Info("notifying peer", "peer", pp.RemoteAddr, "sub", sub)

			ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelFn()

			url := fmt.Sprintf("https://%s/subscription/peer", pp.RemoteAddr)
			rdr := bytes.NewBuffer(data)
			req, err := http.NewRequestWithContext(ctx, "POST", url, rdr)
			if err != nil {
				p.logger.Error("notifying peer (constructing request)", "error", err, "remote", pp.RemoteAddr, "sub", sub)
				return
			}
			req.Header.Add(model.ContentTypeHeader, model.ContentTypeJSON)
			resp, err := p.client.Do(req)
			if err != nil {
				p.logger.Error("notifying peer", "error", err, "remote", pp)
				return
			}

			if resp.StatusCode != http.StatusOK {
				p.logger.Error("bad notify response", "status", resp.StatusCode, "remote", pp.RemoteAddr)
				return
			}

			err = p.store.RemovePendingPeer(pp.RemoteAddr, sub)
			if err != nil {
				p.logger.Error("cleanup", "error", err, "remote", pp, "sub", sub)
				return
			}

			p.store.TouchPeer(pp.RemoteAddr)
		}()
	}
	wg.Wait()

	return nil
}

func (p *peer) handleDeleteSubscription(w http.ResponseWriter, req *http.Request) {
	params := model.SubscriptionRequest{}

	body := req.Body
	defer body.Close()

	dec := json.NewDecoder(body)
	err := dec.Decode(&params)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = p.store.DeleteSubs(req.RemoteAddr, params.Spec)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (p *peer) handleSubscriptionPeerUpdate(w http.ResponseWriter, req *http.Request) {
	body := req.Body
	defer body.Close()

	respData := model.SubscriptionResponse{}
	dec := json.NewDecoder(body)
	err := dec.Decode(&respData)
	if err != nil {
		p.logger.Error("decoding ping response", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for sub, peerList := range respData.Peers {
		// don't include ourselves in the list of peers
		cleanedList := make([]string, 0, len(respData.Peers))
		for _, peer := range peerList {
			if peer == p.remoteAddr {
				continue
			}
			cleanedList = append(cleanedList, peer)
		}
		if len(cleanedList) == 0 {
			continue
		}
		p.logger.Info("peer update notification", "sub", sub, "remoteAddr", peerList)

		p.logger.Debug("upsert peers for sub", "sub", sub, "peers", cleanedList)
		err := p.store.UpsertPeersForSub(sub, cleanedList)
		if err != nil {
			p.logger.Error("upserting peers", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (p *peer) handleCreateAction(w http.ResponseWriter, req *http.Request) {
	p.logger.Info("action", "remote", req.RemoteAddr)

	body := req.Body
	defer body.Close()

	rdr := io.LimitReader(body, MaxBodySize)
	buf, err := io.ReadAll(rdr)
	if err != nil {
		p.logger.Error("reading body", "error", err)
	}

	identifier := req.Header.Get(HeaderIdentifier)
	actionID := req.Header.Get(HeaderActionID)
	encodedSig := req.Header.Get(HeaderSignature)
	nodeID := req.Header.Get(HeaderNodeID)
	action := string(buf)

	sig, err := base64.StdEncoding.DecodeString(encodedSig)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	// TODO: if certificate is not found, fetch it from the sender
	cert, err := p.store.GetCertificate(identifier)
	if err != nil {
		if !errors.Is(err, model.ErrNotFound) {
			p.logger.Error("getting certificate", "error", err, "id", identifier)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}

	h := sha256.New()
	h.Write([]byte(cert.Issuer.CommonName))
	h.Write([]byte(actionID))
	h.Write([]byte(action))

	if ed25519PublicKey, ok := cert.PublicKey.(ed25519.PublicKey); ok {
		if !ed25519.Verify(ed25519PublicKey, h.Sum(nil), sig) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	} else {
		p.logger.Error("unsupported public key type")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = p.store.AddAction(actionID, action, req.RemoteAddr)
	if err != nil {
		if errors.Is(err, model.ErrAlreadyExists) {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		p.logger.Error("storing action", "error", err, "identifier", cert.Issuer.CommonName, "id", actionID, "action", action)
	}

	parser, err := ast.Parse(action)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte("syntax error: " + err.Error()))
		if err != nil {
			p.logger.Error("sending response", "error", err)
		}
		return
	}

	p.actionQueue <- Action{
		ID:         id,
		Timestamp:  time.Now().UTC(),
		NodeID:     nodeID,
		RemoteAddr: p.remoteAddr,
		Action:     action,
		Command:    parser.Command(),
	}

	p.logger.Info("action", "id", id, "action", action)

	w.WriteHeader(http.StatusAccepted)
}

func (p *peer) handlePing(w http.ResponseWriter, req *http.Request) {
	p.logger.Info("ping", "remote", req.RemoteAddr)

	seeds, err := p.store.GetSeeds()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp := model.PingResponse{
		Seeds: make([]string, len(seeds)),
	}

	for i, s := range seeds {
		resp.Seeds[i] = s.RemoteAddr
	}
	// append self
	resp.Seeds = append(resp.Seeds, req.Host)

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
		p.logger.Error("sending pong", "error", err, "remote", addr)
	}

	if resp.StatusCode != http.StatusOK {
		p.logger.Error("bad pong response", "remote", addr)
	}
}

func (p *peer) handlePong(w http.ResponseWriter, req *http.Request) {
	p.logger.Info("pong", "remote", req.RemoteAddr)
	w.WriteHeader(http.StatusOK)
}

func (p *peer) pingSeeds() error {
	seeds, err := p.store.GetSeeds()
	if err != nil {
		return fmt.Errorf("fetching seeds: %w", err)
	}

	if len(seeds) == 0 {
		return nil
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFn()

	wg := sync.WaitGroup{}
	ch := make(chan model.PingResponse, len(seeds))

	for _, seed := range seeds {
		wg.Add(1)
		go func() {
			defer wg.Done()

			p.logger.Debug("pinging seed", "seed", seed.RemoteAddr)

			ctxInner, cancelFnInner := context.WithTimeout(ctx, 5*time.Second)
			defer cancelFnInner()

			url := fmt.Sprintf("https://%s/ping", seed.RemoteAddr)
			req, err := http.NewRequestWithContext(ctxInner, "POST", url, nil)
			if err != nil {
				p.logger.Error("sending ping (constructing request)", "error", err, "remote", seed)
				return
			}
			req.Header.Add(model.ContentTypeHeader, model.ContentTypeJSON)
			resp, err := p.client.Do(req)
			if err != nil {
				p.logger.Error("sending ping", "error", err, "remote", seed)
				return
			}

			if resp.StatusCode != http.StatusOK {
				p.logger.Error("bad ping response", "remote", seed)
				return
			}

			body := resp.Body
			defer body.Close()

			respData := model.PingResponse{}
			dec := json.NewDecoder(body)
			err = dec.Decode(&respData)
			if err != nil {
				p.logger.Error("decoding ping response", "err", err)
				return
			}
			ch <- respData
		}()
	}

	wg.Wait()
	close(ch)

	seedMap := map[string]struct{}{}
	for resp := range ch {
		for _, s := range resp.Seeds {
			if _, ok := seedMap[s]; !ok {
				seedMap[s] = struct{}{}
			}
		}
	}
	seedList := []string{}
	for k := range seedMap {
		seedList = append(seedList, k)
	}
	err = p.store.UpsertSeeds(seedList)
	if err != nil {
		return fmt.Errorf("updating seeds: %w", err)
	}

	if len(seedList) == 0 {
		p.logger.Warn("no seeds found")
	}

	return nil
}

func (p *peer) getInitialPeers() error {
	seeds, err := p.store.GetSeeds()
	if err != nil {
		return fmt.Errorf("fetching peers: %w", err)
	}

	if len(seeds) == 0 {
		return fmt.Errorf("geting peers: no seeds")
	}

	subs, err := p.store.GetSelfSubs()
	if err != nil {
		return fmt.Errorf("fetching peers: %w", err)
	}
	if len(subs) == 0 {
		return nil
	}

	specs := make([]string, len(subs))
	for i, s := range subs {
		specs[i] = s.Spec
	}

	p.logger.Info("fetching peers", "seeds", len(seeds), "subs", len(subs))

	for _, peer := range seeds {
		err = p.sendSub(peer.RemoteAddr, specs)
		if err != nil {
			p.logger.Error("fetching peers", "error", err, "peer", peer, "subs", subs)
			continue
		}
	}

	return nil
}

func (p *peer) refreshSubs() error {
	p.logger.Debug("refreshing subs")
	return nil
}

func (p *peer) sendSub(peer string, subs []string) error {
	p.logger.Debug("sending sub", "peer", peer, "subs", subs)

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
		return fmt.Errorf("create subscription resp code: %d", resp.StatusCode)
	}

	p.remoteAddr = resp.Header.Get(HeaderRemotAddress)

	respData := model.SubscriptionResponse{}
	body := resp.Body
	defer body.Close()

	dec := json.NewDecoder(body)
	err = dec.Decode(&respData)
	if err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	for sub, peerList := range respData.Peers {
		// don't include ourselves in the list of peers
		cleanedList := make([]string, 0, len(respData.Peers))
		for _, peer := range peerList {
			if peer == p.remoteAddr {
				continue
			}
			cleanedList = append(cleanedList, peer)
		}
		if len(cleanedList) == 0 {
			continue
		}

		p.logger.Debug("upsert peers for sub", "sub", sub, "peers", cleanedList)
		err := p.store.UpsertPeersForSub(sub, cleanedList)
		if err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
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

func (p *peer) SendAction(action string) error {
	peers, err := p.store.GetPeers()
	if err != nil {
		return fmt.Errorf("getting peers: %w", err)
	}

	if len(peers) == 0 {
		return fmt.Errorf("no peers available")
	}

	id, err := gonanoid.New()
	if err != nil {
		return fmt.Errorf("send action: generating id: %w", err)
	}

	err = p.store.AddAction(id, action, SelfRemoteAddress)
	if err != nil {
		return fmt.Errorf("send action: saving action: %w", err)
	}

	buf := bytes.NewBufferString(action)

	ctx, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFn()

	wg := sync.WaitGroup{}
	for _, peer := range peers {
		if peer.RemoteAddr == p.remoteAddr {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			ctxInner, cancelFnInner := context.WithTimeout(ctx, 5*time.Second)
			defer cancelFnInner()

			url := fmt.Sprintf("https://%s/action", peer.RemoteAddr)
			req, err := http.NewRequestWithContext(ctxInner, "POST", url, buf)
			req.Header.Add(HeaderActionID, id)
			req.Header.Add(HeaderSender, "TODO")
			req.Header.Add(HeaderSignature, "TODO")

			if err != nil {
				p.logger.Error("send action: creating action request", "error", err, "remote", peer.RemoteAddr)
				return
			}

			resp, err := p.client.Do(req)
			if err != nil {
				p.logger.Error("send action: executing action request", "error", err, "remote", peer.RemoteAddr)
				return
			}

			if resp.StatusCode != http.StatusAccepted {
				p.logger.Error("send action: action request not accepted", "error", err, "remote", peer.RemoteAddr)
				return
			}

			err = p.store.TouchPeer(peer.RemoteAddr)
			p.logger.Error("send action: touching peer", "error", err, "remote", peer.RemoteAddr)
		}()
	}
	wg.Wait()

	return nil
}
