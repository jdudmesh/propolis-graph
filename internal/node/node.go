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
	"strings"
	"sync"
	"time"

	"github.com/jdudmesh/propolis/internal/ast"
	"github.com/jdudmesh/propolis/internal/graph"
	"github.com/jdudmesh/propolis/internal/identity"
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

type Executor interface {
	Execute(stmt any) (any, error)
}

type Action struct {
	ID          string
	RemoteAddr  string
	NodeID      string
	Timestamp   time.Time
	Action      string
	Command     ast.Command
	Certificate x509.Certificate
}

type node struct {
	nodeID             string
	host               string
	port               int
	store              *store
	logger             *slog.Logger
	roundTripper       *http3.RoundTripper
	server             *http3.Server
	client             *http.Client
	notifyPendingPeers chan string
	actionQueue        chan Action
	quit               chan struct{}
	remoteAddr         string
	nodeType           model.NodeType
	executor           Executor
}

func New(config model.NodeConfig) (*node, error) {
	store, err := newStore(config.NodeDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("creating store: %w", err)
	}

	executor, err := graph.New(config)
	if err != nil {
		return nil, fmt.Errorf("creating executor: %w", err)
	}

	nodeID, err := gonanoid.New()
	if err != nil {
		return nil, fmt.Errorf("generating node id: %w", err)
	}

	n := &node{
		nodeID:             nodeID,
		host:               config.Host,
		port:               config.Port,
		store:              store,
		logger:             config.Logger,
		nodeType:           config.Type,
		executor:           executor,
		notifyPendingPeers: make(chan string),
		actionQueue:        make(chan Action),
		quit:               make(chan struct{}),
	}

	n.server = &http3.Server{
		Handler: n.newServeMux(),
	}

	return n, nil
}

func (n *node) SetInitialSeeds(seeds []string) error {
	return n.store.UpsertSeeds(seeds)
}

func (n *node) SetInitialSubscriptions(subs []string) error {
	return n.store.InitSubs(subs)
}

func (n *node) newServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	switch n.nodeType {
	case model.NodeTypeSeed:
		fallthrough
	case model.NodeTypePeer:
		mux.HandleFunc("POST /subscription", n.handleCreateSubscription)
		mux.HandleFunc("DELETE /subscription", n.handleDeleteSubscription)
		mux.HandleFunc("POST /subscription/peer", n.handleSubscriptionPeerUpdate)
		mux.HandleFunc("POST /ping", n.handlePing)
		mux.HandleFunc("POST /pong", n.handlePong)
		mux.HandleFunc("GER /certificate/{id}", n.handleGetCertificate)
	case model.NodeTypeCache:
		mux.HandleFunc("POST /action", n.handleCreateAction)
	}
	return mux
}

func (n *node) Run() error {
	defer n.server.CloseGracefully(10 * time.Second)

	addr := &net.UDPAddr{IP: net.ParseIP(n.host), Port: n.port}
	switch n.nodeType {
	case model.NodeTypePeer:
		n.logger.Info("starting peer", "addr", addr)
	case model.NodeTypeSeed:
		n.logger.Info("starting seed", "addr", addr)
	}

	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("creating sock: %w", err)
	}

	tr := quic.Transport{
		Conn: udpConn,
	}
	defer tr.Close()

	n.roundTripper = &http3.RoundTripper{
		TLSClientConfig: &tls.Config{
			NextProtos:         []string{"h3", "propolis"},
			InsecureSkipVerify: true,
		},
		QUICConfig: &quic.Config{}, // QUIC connection options
		Dial: func(ctx context.Context, addr string, tlsConf *tls.Config, quicConf *quic.Config) (quic.EarlyConnection, error) {
			n.logger.Debug("dialing", "addr", addr)
			a, err := net.ResolveUDPAddr("udp", addr)
			if err != nil {
				return nil, err
			}
			return tr.DialEarly(ctx, a, tlsConf, quicConf)
		},
	}
	defer n.roundTripper.Close()

	n.client = &http.Client{
		Transport: n.roundTripper,
	}

	listener, err := tr.ListenEarly(n.generateTLSConfig(), nil)
	if err != nil {
		return fmt.Errorf("setting up listener sock: %w", err)
	}

	go func() {
		err := n.server.ServeListener(listener)
		if err != nil {
			n.logger.Error("closing peer server", "error", err)
		}
	}()

	err = n.pingSeeds()
	if err != nil {
		return fmt.Errorf("refreshing seeds: %w", err)
	}

	switch n.nodeType {
	case model.NodeTypePeer:
		return n.runLoopPeer()
	case model.NodeTypeSeed:
		return n.runLoopSeed()
	case model.NodeTypeCache:
		return n.runLoopCache()
	}

	return nil
}

func (n *node) runLoopPeer() error {
	err := n.getInitialPeers()
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
			err := n.refreshSubs()
			if err != nil {
				n.logger.Error("refreshing subscriptions", "error", err)
			}
			n.roundTripper.CloseIdleConnections()
		case <-t2.C:
			err := n.pingSeeds()
			if err != nil {
				n.logger.Error("refreshing seeds", "error", err)
			}
		case <-n.quit:
			return nil
		}
	}
}

func (n *node) runLoopSeed() error {
	t1 := time.NewTicker(5 * time.Second)
	defer t1.Stop()
	t2 := time.NewTicker(60 * time.Minute)
	defer t2.Stop()

	for {
		select {
		case sub := <-n.notifyPendingPeers:
			err := n.doNotifyPendingPeers(sub)
			if err != nil {
				n.logger.Error("notifying peers", "error", err)
			}
		case <-t1.C:
			n.roundTripper.CloseIdleConnections()
		case <-t2.C:
			err := n.pingSeeds()
			if err != nil {
				n.logger.Error("refreshing seeds", "error", err)
			}
		case <-n.quit:
			return nil
		}
	}
}

func (n *node) runLoopCache() error {
	dispatchQueue := make(chan any)
	defer close(dispatchQueue)

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
		case action := <-n.actionQueue:

			res, err := n.executor.Execute(action.Command)
			if err != nil {
				n.logger.Error("executing action", "error", err)
				continue
			}
			dispatchQueue <- res
		case <-n.quit:
			break outer
		}
	}

	close(dispatchQueue)
	wg.Wait()

	return nil
}

func (n *node) Close() error {
	close(n.quit)
	return nil
}

func (n *node) handleCreateSubscription(w http.ResponseWriter, req *http.Request) {
	params := model.SubscriptionRequest{}

	body := req.Body
	defer body.Close()

	dec := json.NewDecoder(body)
	err := dec.Decode(&params)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	n.logger.Debug("upsert sub for peer", "sub", params.Spec, "peer", req.RemoteAddr)
	err = n.store.UpsertSubs(req.RemoteAddr, params.Spec)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp := &model.SubscriptionResponse{
		Peers: make(map[string][]string),
	}

	for _, s := range params.Spec {
		peers, err := n.store.FindPeersBySub(s)
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
			n.logger.Debug("adding pending peer", "remoteAddr", req.RemoteAddr, "sub", s)
			n.store.AddPendingPeer(req.RemoteAddr, s)
		} else {
			n.notifyPendingPeers <- s
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

func (n *node) doNotifyPendingPeers(sub string) error {
	pendingPeers, err := n.store.GetPendingPeersForSub(sub)
	if err != nil {
		return fmt.Errorf("fetching pending peers: %w", err)
	}

	if len(pendingPeers) == 0 {
		return nil
	}
	n.logger.Debug("notifying pending peers", "n", len(pendingPeers))

	notification := &model.SubscriptionResponse{
		Peers: make(map[string][]string),
	}

	peers, err := n.store.FindPeersBySub(sub)
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

			n.logger.Info("notifying peer", "peer", pp.RemoteAddr, "sub", sub)

			ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelFn()

			url := fmt.Sprintf("https://%s/subscription/peer", pp.RemoteAddr)
			rdr := bytes.NewBuffer(data)
			req, err := http.NewRequestWithContext(ctx, "POST", url, rdr)
			if err != nil {
				n.logger.Error("notifying peer (constructing request)", "error", err, "remote", pp.RemoteAddr, "sub", sub)
				return
			}
			req.Header.Add(model.ContentTypeHeader, model.ContentTypeJSON)
			resp, err := n.client.Do(req)
			if err != nil {
				n.logger.Error("notifying peer", "error", err, "remote", pp)
				return
			}

			if resp.StatusCode != http.StatusOK {
				n.logger.Error("bad notify response", "status", resp.StatusCode, "remote", pp.RemoteAddr)
				return
			}

			err = n.store.RemovePendingPeer(pp.RemoteAddr, sub)
			if err != nil {
				n.logger.Error("cleanup", "error", err, "remote", pp, "sub", sub)
				return
			}

			n.store.TouchPeer(pp.RemoteAddr)
		}()
	}
	wg.Wait()

	return nil
}

func (n *node) handleDeleteSubscription(w http.ResponseWriter, req *http.Request) {
	params := model.SubscriptionRequest{}

	body := req.Body
	defer body.Close()

	dec := json.NewDecoder(body)
	err := dec.Decode(&params)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = n.store.DeleteSubs(req.RemoteAddr, params.Spec)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (n *node) handleSubscriptionPeerUpdate(w http.ResponseWriter, req *http.Request) {
	body := req.Body
	defer body.Close()

	respData := model.SubscriptionResponse{}
	dec := json.NewDecoder(body)
	err := dec.Decode(&respData)
	if err != nil {
		n.logger.Error("decoding ping response", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for sub, peerList := range respData.Peers {
		// don't include ourselves in the list of peers
		cleanedList := make([]string, 0, len(respData.Peers))
		for _, peer := range peerList {
			if peer == n.remoteAddr {
				continue
			}
			cleanedList = append(cleanedList, peer)
		}
		if len(cleanedList) == 0 {
			continue
		}
		n.logger.Info("peer update notification", "sub", sub, "remoteAddr", peerList)

		n.logger.Debug("upsert peers for sub", "sub", sub, "peers", cleanedList)
		err := n.store.UpsertPeersForSub(sub, cleanedList)
		if err != nil {
			n.logger.Error("upserting peers", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (n *node) handleCreateAction(w http.ResponseWriter, req *http.Request) {
	n.logger.Info("action", "remote", req.RemoteAddr)

	body := req.Body
	defer body.Close()

	rdr := io.LimitReader(body, MaxBodySize)
	buf, err := io.ReadAll(rdr)
	if err != nil {
		n.logger.Error("reading body", "error", err)
	}

	identity := req.Header.Get(HeaderIdentifier)
	actionID := req.Header.Get(HeaderActionID)
	encodedSig := req.Header.Get(HeaderSignature)
	nodeID := req.Header.Get(HeaderNodeID)
	action := string(buf)

	sig, err := base64.StdEncoding.DecodeString(encodedSig)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	// TODO: if certificate is not found, fetch it from the sender
	cert, err := n.store.GetCachedCertificate(identity)
	if err != nil {
		if !errors.Is(err, model.ErrNotFound) {
			n.logger.Error("getting certificate", "error", err, "id", identity)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		cert, err = n.requestCertificate(identity, req.RemoteAddr)
		if err != nil {
			n.logger.Error("fetching certificate", "error", err, "id", identity)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
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
		n.logger.Error("unsupported public key type")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = n.store.AddAction(actionID, action, req.RemoteAddr)
	if err != nil {
		if errors.Is(err, model.ErrAlreadyExists) {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		n.logger.Error("storing action", "error", err, "identifier", cert.Issuer.CommonName, "id", actionID, "action", action)
	}

	parser, err := ast.Parse(action)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte("syntax error: " + err.Error()))
		if err != nil {
			n.logger.Error("sending response", "error", err)
		}
		return
	}

	n.actionQueue <- Action{
		ID:          actionID,
		Timestamp:   time.Now().UTC(),
		NodeID:      nodeID,
		RemoteAddr:  n.remoteAddr,
		Certificate: *cert,
		Action:      action,
		Command:     parser.Command(),
	}

	n.logger.Info("action", "id", actionID, "action", action)

	w.WriteHeader(http.StatusAccepted)
}

func (n *node) handlePing(w http.ResponseWriter, req *http.Request) {
	n.logger.Info("ping", "remote", req.RemoteAddr)

	seeds, err := n.store.GetSeeds()
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

func (n *node) sendPong(addr string) {
	resp, err := n.client.Post(fmt.Sprintf("https://%s/pong", addr), model.ContentTypeJSON, nil)
	if err != nil {
		n.logger.Error("sending pong", "error", err, "remote", addr)
	}

	if resp.StatusCode != http.StatusOK {
		n.logger.Error("bad pong response", "remote", addr)
	}
}

func (n *node) handlePong(w http.ResponseWriter, req *http.Request) {
	n.logger.Info("pong", "remote", req.RemoteAddr)
	w.WriteHeader(http.StatusOK)
}

func (n *node) pingSeeds() error {
	seeds, err := n.store.GetSeeds()
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

			n.logger.Debug("pinging seed", "seed", seed.RemoteAddr)

			ctxInner, cancelFnInner := context.WithTimeout(ctx, 5*time.Second)
			defer cancelFnInner()

			url := fmt.Sprintf("https://%s/ping", seed.RemoteAddr)
			req, err := http.NewRequestWithContext(ctxInner, "POST", url, nil)
			if err != nil {
				n.logger.Error("sending ping (constructing request)", "error", err, "remote", seed)
				return
			}
			req.Header.Add(model.ContentTypeHeader, model.ContentTypeJSON)
			resp, err := n.client.Do(req)
			if err != nil {
				n.logger.Error("sending ping", "error", err, "remote", seed)
				return
			}

			if resp.StatusCode != http.StatusOK {
				n.logger.Error("bad ping response", "remote", seed)
				return
			}

			body := resp.Body
			defer body.Close()

			respData := model.PingResponse{}
			dec := json.NewDecoder(body)
			err = dec.Decode(&respData)
			if err != nil {
				n.logger.Error("decoding ping response", "err", err)
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
	err = n.store.UpsertSeeds(seedList)
	if err != nil {
		return fmt.Errorf("updating seeds: %w", err)
	}

	if len(seedList) == 0 {
		n.logger.Warn("no seeds found")
	}

	return nil
}

func (n *node) getInitialPeers() error {
	seeds, err := n.store.GetSeeds()
	if err != nil {
		return fmt.Errorf("fetching peers: %w", err)
	}

	if len(seeds) == 0 {
		return fmt.Errorf("geting peers: no seeds")
	}

	subs, err := n.store.GetSelfSubs()
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

	n.logger.Info("fetching peers", "seeds", len(seeds), "subs", len(subs))

	for _, peer := range seeds {
		err = n.sendSub(peer.RemoteAddr, specs)
		if err != nil {
			n.logger.Error("fetching peers", "error", err, "peer", peer, "subs", subs)
			continue
		}
	}

	return nil
}

func (n *node) refreshSubs() error {
	n.logger.Debug("refreshing subs")
	return nil
}

func (n *node) sendSub(peer string, subs []string) error {
	n.logger.Debug("sending sub", "peer", peer, "subs", subs)

	params := model.SubscriptionRequest{
		Spec: subs,
	}

	data, err := json.Marshal(&params)
	if err != nil {
		return fmt.Errorf("marshalling subscription req: %w", err)
	}

	buf := bytes.NewBuffer(data)
	resp, err := n.client.Post(fmt.Sprintf("https://%s/subscription", peer), model.ContentTypeJSON, buf)
	if err != nil {
		return fmt.Errorf("sending subscription req: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create subscription resp code: %d", resp.StatusCode)
	}

	n.remoteAddr = resp.Header.Get(HeaderRemotAddress)

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
			if peer == n.remoteAddr {
				continue
			}
			cleanedList = append(cleanedList, peer)
		}
		if len(cleanedList) == 0 {
			continue
		}

		n.logger.Debug("upsert peers for sub", "sub", sub, "peers", cleanedList)
		err := n.store.UpsertPeersForSub(sub, cleanedList)
		if err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

func (n *node) generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{
		Subject: pkix.Name{
			CommonName: n.nodeID,
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

func (n *node) SendIdentity(id *identity.Identity) error {
	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: id.CertificateData}))
	certPEMEncoded, err := json.Marshal(certPEM)
	if err != nil {
		return fmt.Errorf("marshalling certificate: %w", err)
	}

	sb := strings.Builder{}
	sb.WriteString("MERGE (:Identity{")
	props := []string{
		fmt.Sprintf("id:'%s'", id.Identifier),
		fmt.Sprintf("handle:'%s'", id.Handle),
		fmt.Sprintf("bio:'%s'", id.Bio),
		fmt.Sprintf("certificate:'%s'", string(certPEMEncoded)),
	}
	sb.WriteString(strings.Join(props, ", "))
	sb.WriteString("})")

	err = n.SendAction(id, sb.String())
	if err != nil {
		return err
	}

	return nil
}

func (n *node) SendAction(id *identity.Identity, action string) error {
	var privateKey ed25519.PrivateKey
	for _, key := range id.Keys {
		if key.Type == identity.KeyTypeED25519PrivateKey {
			privateKey = key.Data
			break
		}
	}
	if privateKey == nil {
		return fmt.Errorf("private key not found")
	}

	cert, err := x509.ParseCertificate(id.CertificateData)
	if err != nil {
		return fmt.Errorf("parsing certificate: %w", err)
	}
	n.store.PutCachedCertificate(cert)

	actionID := gonanoid.Must()

	h := sha256.New()
	h.Write([]byte(id.Identifier))
	h.Write([]byte(actionID))
	h.Write([]byte(action))
	sig := ed25519.Sign(privateKey, h.Sum(nil))
	encodedSig := base64.StdEncoding.EncodeToString(sig)

	buf := bytes.NewBufferString(action)

	ctx, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFn()

	peers, err := n.store.GetPeers()
	if err != nil {
		return fmt.Errorf("getting peers: %w", err)
	}

	if len(peers) == 0 {
		return fmt.Errorf("no peers available")
	}

	err = n.store.AddAction(actionID, action, SelfRemoteAddress)
	if err != nil {
		return fmt.Errorf("send action: saving action: %w", err)
	}

	wg := sync.WaitGroup{}
	for _, peer := range peers {
		if peer.RemoteAddr == n.remoteAddr {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			ctxInner, cancelFnInner := context.WithTimeout(ctx, 5*time.Second)
			defer cancelFnInner()

			url := fmt.Sprintf("https://%s/action", peer.RemoteAddr)
			req, err := http.NewRequestWithContext(ctxInner, "POST", url, buf)
			req.Header.Add(HeaderIdentifier, id.Identifier)
			req.Header.Add(HeaderActionID, actionID)
			req.Header.Add(HeaderNodeID, n.nodeID)
			req.Header.Add(HeaderSignature, encodedSig)

			if err != nil {
				n.logger.Error("send action: creating action request", "error", err, "remote", peer.RemoteAddr)
				return
			}

			resp, err := n.client.Do(req)
			if err != nil {
				n.logger.Error("send action: executing action request", "error", err, "remote", peer.RemoteAddr)
				return
			}

			if resp.StatusCode != http.StatusAccepted {
				n.logger.Error("send action: action request not accepted", "error", err, "remote", peer.RemoteAddr)
				return
			}

			err = n.store.TouchPeer(peer.RemoteAddr)
			n.logger.Error("send action: touching peer", "error", err, "remote", peer.RemoteAddr)
		}()
	}
	wg.Wait()

	return nil
}

func (n *node) handleGetCertificate(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	n.logger.Info("get certificate", "id", id)

	cert, err := n.store.GetCachedCertificate(id)
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	w.Header().Add(model.ContentTypeHeader, "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (n *node) requestCertificate(identifier, remoteAddr string) (*x509.Certificate, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFn()

	url := fmt.Sprintf("https://%s/certificate/%s", remoteAddr, identifier)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating certificate request: %w", err)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing certificate request: %w", err)
	}

	body := resp.Body
	defer body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad certificate response: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading certificate response: %w", err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("decoding certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing certificate: %w", err)
	}

	return cert, nil
}
