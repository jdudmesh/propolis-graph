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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
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
	"github.com/jdudmesh/propolis/internal/bloom"
	"github.com/jdudmesh/propolis/internal/graph"
	"github.com/jdudmesh/propolis/internal/identity"
	"github.com/jdudmesh/propolis/internal/model"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

const (
	MaxBodySize = 1048576

	HeaderRemoteAddress = "x-propolis-remote-address"
	HeaderActionID      = "x-propolis-action-id"
	HeaderNodeID        = "x-propolis-node-id"
	HeaderSender        = "x-propolis-sender"
	HeaderSignature     = "x-propolis-signature"
	HeaderIdentifier    = "x-propolis-identifier"
	HeaderReceivedFrom  = "x-propolis-received-from"

	SelfRemoteAddress = "0.0.0.0"
	MaxPeers          = 5
)

type Graph interface {
	Execute(stmt any) (any, error)
}

type Action struct {
	ID               string
	RemoteAddr       string
	NodeID           string
	Identity         string
	Timestamp        time.Time
	Action           string
	Command          ast.Command
	Certificate      x509.Certificate
	ReceivedFrom     string
	EncodedSignature string
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
	publicAddr         string
	nodeType           NodeType
	executor           Graph
	subscriptions      *bloom.Filter
	seeds              []string
	identity           identity.Identity
}

func New(config Config, subscriptions *bloom.Filter) (*node, error) {
	if subscriptions == nil {
		subscriptions = bloom.New()
	}

	store, err := newStore(config.NodeDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("creating store: %w", err)
	}

	executor, err := graph.New(config.Config)
	if err != nil {
		return nil, fmt.Errorf("creating executor: %w", err)
	}

	publicAddr := config.PublicAddress
	if publicAddr == "" && config.Type == NodeTypeSeed {
		publicAddr = fmt.Sprintf("%s:%d", config.Host, config.Port)
	}

	n := &node{
		nodeID:             model.NewID(),
		host:               config.Host,
		port:               config.Port,
		publicAddr:         publicAddr,
		store:              store,
		logger:             config.Logger,
		nodeType:           config.Type,
		executor:           executor,
		notifyPendingPeers: make(chan string),
		actionQueue:        make(chan Action),
		quit:               make(chan struct{}),
		subscriptions:      subscriptions,
		seeds:              config.Seeds,
		identity:           config.Identity,
	}

	n.server = &http3.Server{
		Handler: n.newServeMux(),
	}

	return n, nil
}

func (n *node) setInitialSeeds() error {
	s := make([]*model.SeedSpec, 0, len(n.seeds))
	for _, seed := range n.seeds {
		spec, err := n.getNodeInfo(seed)
		if err != nil {
			n.logger.Error("getting seed info", "error", err)
			continue
		}

		s = append(s, &model.SeedSpec{
			CreatedAt:  spec.CreatedAt,
			UpdatedAt:  spec.UpdatedAt,
			RemoteAddr: seed,
			NodeID:     spec.NodeID,
		})
	}
	return n.store.UpsertSeeds(s)
}

func (n *node) getNodeInfo(remoteAddr string) (*model.PeerSpec, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://%s/whoami", remoteAddr), nil)
	if err != nil {
		return nil, fmt.Errorf("creating whoami request: %w", err)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("getting whoami: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad whoami response: %d", resp.StatusCode)
	}

	if n.publicAddr == "" {
		n.publicAddr = resp.Header.Get(HeaderRemoteAddress)
	}

	body := resp.Body
	defer body.Close()

	spec := &model.PeerSpec{}
	dec := json.NewDecoder(body)
	err = dec.Decode(spec)
	if err != nil {
		return nil, fmt.Errorf("decoding whoami: %w", err)
	}

	return spec, nil
}

func (n *node) newServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	switch n.nodeType {
	case NodeTypeSeed:
		mux.HandleFunc("POST /hello", n.handleJoin)
		mux.HandleFunc("POST /goodbye", n.handleLeave)
		mux.HandleFunc("GET /whois/{id}", n.handleWhoIs)
		mux.HandleFunc("GET /whoami", n.handleWhoAmI)
	case NodeTypePeer:
		// mux.HandleFunc("POST /subscription", n.handleCreateSubscription)
		// mux.HandleFunc("DELETE /subscription", n.handleDeleteSubscription)
		// mux.HandleFunc("POST /subscription/peer", n.handleSubscriptionPeerUpdate)
		mux.HandleFunc("POST /ping", n.handlePing)
		mux.HandleFunc("POST /pong", n.handlePong)
		mux.HandleFunc("GET /whois/{id}", n.handleWhoIs)
		mux.HandleFunc("POST /publish", n.handlePublish)
	}
	return mux
}

func (n *node) Run() error {
	defer n.server.CloseGracefully(10 * time.Second)

	addr := &net.UDPAddr{IP: net.ParseIP(n.host), Port: n.port}
	switch n.nodeType {
	case NodeTypePeer:
		n.logger.Info("starting peer", "addr", addr)
	case NodeTypeSeed:
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

	switch n.nodeType {
	case NodeTypePeer:
		return n.runLoopPeer()
	case NodeTypeSeed:
		return n.runLoopSeed()
	case NodeTypeCache:
		return n.runLoopCache()
	}

	return nil
}

func (n *node) runLoopPeer() error {
	defer n.leaveSeeds()

	err := n.setInitialSeeds()
	if err != nil {
		return fmt.Errorf("setting initial seeds: %w", err)
	}

	err = n.joinSeeds()
	if err != nil {
		return fmt.Errorf("joining: %w", err)
	}

	// t1 := time.NewTicker(5 * time.Second)
	// defer t1.Stop()
	t2 := time.NewTicker(time.Minute)
	defer t2.Stop()

	for {
		select {
		// case <-t1.C:
		// err := n.refreshSubs()
		// if err != nil {
		// 	n.logger.Error("refreshing subscriptions", "error", err)
		// }
		case <-t2.C:
			go func() {
				err := n.joinSeeds()
				if err != nil {
					n.logger.Error("refreshing seeds", "error", err)
				}
			}()
			go func() {
				err = n.pingPeers()
				if err != nil {
					n.logger.Error("pinging peers", "error", err)
				}
			}()
			n.roundTripper.CloseIdleConnections()
		case <-n.quit:
			return nil
		}
	}
}

func (n *node) runLoopSeed() error {
	err := n.setInitialSeeds()
	if err != nil {
		return fmt.Errorf("setting initial seeds: %w", err)
	}

	// t1 := time.NewTicker(5 * time.Second)
	// defer t1.Stop()
	t2 := time.NewTicker(time.Minute)
	defer t2.Stop()

	for {
		select {
		// case sub := <-n.notifyPendingPeers:
		// 	err := n.doNotifyPendingPeers(sub)
		// 	if err != nil {
		// 		n.logger.Error("notifying peers", "error", err)
		// 	}
		// case <-t1.C:
		// 	n.roundTripper.CloseIdleConnections()
		case <-t2.C:
			err := n.tidyPeers()
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

func (n *node) handleJoin(w http.ResponseWriter, req *http.Request) {
	n.logger.Debug("join", "remote", req.RemoteAddr)

	seeds, err := n.store.GetSeeds()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// add us to the seeds
	seeds = append(seeds, &model.SeedSpec{
		CreatedAt:  time.Now().UTC(),
		RemoteAddr: n.publicAddr,
		NodeID:     n.nodeID,
	})

	peers, err := n.store.GetRandomPeers(req.RemoteAddr, MaxPeers)
	if err != nil {
		n.logger.Error("fetching peers", "error", err, "remote", req.RemoteAddr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	nodeID := req.Header.Get(HeaderNodeID)

	body := req.Body
	defer body.Close()
	rdr := io.LimitReader(body, bloom.FilterLen)
	f, err := io.ReadAll(rdr)
	if err != nil {
		n.logger.Error("reading body", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	b := bloom.New()
	err = b.Parse(string(f))
	if err != nil {
		n.logger.Error("parsing filter", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = n.store.UpsertPeer(model.PeerSpec{
		RemoteAddr: req.RemoteAddr,
		CreatedAt:  time.Now().UTC(),
		NodeID:     nodeID,
		Filter:     b.String(),
	})

	if err != nil {
		n.logger.Error("upserting peer", "error", err, "remote", req.RemoteAddr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp := model.JoinResponse{
		Seeds: seeds,
		Peers: peers,
	}

	data, err := json.Marshal(&resp)
	if err != nil {
		n.logger.Error("marshalling response", "error", err, "remote", req.RemoteAddr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Header().Add(ContentTypeHeader, ContentTypeJSON)
	w.Header().Add(HeaderRemoteAddress, req.RemoteAddr)
	w.Write(data)

	//go n.notifyPeers(peers, req.RemoteAddr)
}

// func (n *node) notifyPeers(peers []*model.PeerSpec, newPeer string) error {
// 	wg := sync.WaitGroup{}
// 	for _, p := range peers {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
// 			defer cancelFn()
// 			req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://%s/peer", p.RemoteAddr), nil)
// 			if err != nil {
// 				n.logger.Error("creating peer req", "error", err, "remote", p.RemoteAddr)
// 				return
// 			}
// 			resp, err := n.client.Do(req)
// 			if err != nil {
// 				n.logger.Error("notifying peer", "error", err, "remote", p.RemoteAddr)

// 				return
// 			}
// 		}()

// 	}
// 	return nil
// }

func (n *node) handleLeave(w http.ResponseWriter, req *http.Request) {
	n.logger.Info("leave", "remote", req.RemoteAddr)
	err := n.store.DeletePeer(req.RemoteAddr)
	if err != nil {
		n.logger.Error("deleting peer", "error", err, "remote", req.RemoteAddr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (n *node) handlePublish(w http.ResponseWriter, req *http.Request) {
	body := req.Body
	defer body.Close()

	rdr := io.LimitReader(body, MaxBodySize)
	buf, err := io.ReadAll(rdr)
	if err != nil {
		n.logger.Error("reading body", "error", err)
	}

	action := Action{
		ID:               req.Header.Get(HeaderActionID),
		RemoteAddr:       req.RemoteAddr,
		NodeID:           req.Header.Get(HeaderNodeID),
		Identity:         req.Header.Get(HeaderIdentifier),
		Timestamp:        time.Now().UTC(),
		Action:           string(buf),
		ReceivedFrom:     req.Header.Get(HeaderReceivedFrom),
		EncodedSignature: req.Header.Get(HeaderSignature),
	}

	n.logger.Info("action", "data", action)

	isProcessed, err := n.store.IsActionProcessed(action.ID)
	if err != nil {
		n.logger.Error("checking action", "error", err, "id", action.ID)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if isProcessed {
		w.WriteHeader(http.StatusFound)
		return
	}

	err = n.verifyAction(action)
	switch {
	case err == identity.ErrUnsupportedPublicKey:
		w.WriteHeader(http.StatusInternalServerError)
		return
	case err == identity.ErrUnauthorized:
		w.WriteHeader(http.StatusUnauthorized)
		return
	case err == identity.ErrBadSignature:
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad signature"))
		return
	case err != nil:
		n.logger.Error("verifying action", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = n.store.CreateAction(action)
	if err != nil {
		n.logger.Error("storing action", "error", err, "action", action)
	}

	parser, err := ast.Parse(action.Action)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte("syntax error: " + err.Error()))
		if err != nil {
			n.logger.Error("sending response", "error", err)
		}
		return
	}

	action.Command = parser.Command()

	// TODO: for now all command must have an explicit identifier for each node
	entityIDs := parser.Identifiers()
	if len(entityIDs) == 0 {
		n.logger.Warn("no identifiers found")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = n.moderateAction(action)
	if err != nil {
		if errors.Is(err, model.ErrNotAcceptable) {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		n.logger.Error("moderating action", "error", err, "action", action)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	n.logger.Debug("action accepted", "action", action)

	// action is relevant to this node if any of the entity IDs are in the subscription filter
	isRelevant := false
	for _, id := range entityIDs {
		if n.subscriptions.Intersects([]byte(id)) {
			isRelevant = true
			break
		}
	}

	if isRelevant {
		n.actionQueue <- action
	}

	// propagate action to peers
	go n.propagateAction(action, entityIDs...)
}

func (n *node) handlePing(w http.ResponseWriter, req *http.Request) {
	n.logger.Info("got ping", "remote", req.RemoteAddr)

	w.Header().Add(HeaderRemoteAddress, req.RemoteAddr)
	w.WriteHeader(http.StatusOK)

	body := req.Body
	defer body.Close()
	rdr := io.LimitReader(body, bloom.FilterLen)
	f, err := io.ReadAll(rdr)
	if err != nil {
		n.logger.Error("reading body", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	b := bloom.New()
	err = b.Parse(string(f))
	if err != nil {
		n.logger.Error("parsing filter", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = n.store.TouchPeer(req.RemoteAddr, b.String())
	if err != nil {
		n.logger.Error("touching peer", "error", err, "remote", req.RemoteAddr)
	}

	go n.sendPong(req.RemoteAddr)
}

func (n *node) sendPong(addr string) {
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://%s/pong", addr), nil)
	if err != nil {
		n.logger.Error("creating pong", "error", err, "remote", addr)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		n.logger.Error("sending pong", "error", err, "remote", addr)
		err = n.store.DeletePeer(addr)
		if err != nil {
			n.logger.Error("deleting peer", "error", err, "remote", addr)
		}
		return
	}

	if resp.StatusCode != http.StatusOK {
		n.logger.Error("bad pong response", "remote", addr)
		err = n.store.DeletePeer(addr)
		if err != nil {
			n.logger.Error("deleting peer", "error", err, "remote", addr)
		}
	}
}

func (n *node) handlePong(w http.ResponseWriter, req *http.Request) {
	n.logger.Info("got pong", "remote", req.RemoteAddr)
	w.WriteHeader(http.StatusOK)
}

func (n *node) joinSeeds() error {
	seeds, err := n.store.GetSeeds()
	if err != nil {
		return fmt.Errorf("join seeds (fetching seeds): %w", err)
	}

	if len(seeds) == 0 {
		return nil
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFn()

	wg := sync.WaitGroup{}
	ch := make(chan model.JoinResponse, len(seeds))

	subs := n.subscriptions.String()
	for _, seed := range seeds {
		wg.Add(1)
		go func() {
			defer wg.Done()

			n.logger.Debug("joining seed", "seed", seed.RemoteAddr)

			ctxInner, cancelFnInner := context.WithTimeout(ctx, 5*time.Second)
			defer cancelFnInner()

			url := fmt.Sprintf("https://%s/hello", seed.RemoteAddr)
			buf := bytes.NewBufferString(subs)
			req, err := http.NewRequestWithContext(ctxInner, "POST", url, buf)
			if err != nil {
				n.logger.Error("sending hello (constructing request)", "error", err, "remote", seed)
				return
			}
			req.Header.Add(HeaderNodeID, n.nodeID)

			resp, err := n.client.Do(req)
			if err != nil {
				n.logger.Error("sending hello", "error", err, "remote", seed)
				return
			}

			if resp.StatusCode != http.StatusAccepted {
				n.logger.Error("bad hellop response", "remote", seed, "status", resp.StatusCode)
				return
			}

			body := resp.Body
			defer body.Close()

			respData := model.JoinResponse{}
			dec := json.NewDecoder(body)
			err = dec.Decode(&respData)
			if err != nil {
				n.logger.Error("decoding ping response", "err", err)
				return
			}

			n.logger.Debug("join response", "seeds", len(respData.Seeds), "peers", len(respData.Peers))

			err = n.store.TouchSeed(seed.RemoteAddr)
			if err != nil {
				n.logger.Error("touching seed", "error", err, "remote", seed.RemoteAddr)
			}

			ch <- respData
		}()
	}

	wg.Wait()
	close(ch)

	seedMap := map[string]*model.SeedSpec{}
	peerMap := map[string]*model.PeerSpec{}
	for resp := range ch {
		for _, s := range resp.Seeds {
			if _, ok := seedMap[s.RemoteAddr]; !ok {
				seedMap[s.RemoteAddr] = s
			}
		}
		for _, p := range resp.Peers {
			if _, ok := peerMap[p.RemoteAddr]; !ok {
				peerMap[p.RemoteAddr] = p
			}
		}
	}

	seedList := []*model.SeedSpec{}
	for _, v := range seedMap {
		seedList = append(seedList, v)
	}

	if len(seedList) == 0 {
		n.logger.Warn("no seeds found")
	}

	err = n.store.UpsertSeeds(seedList)
	if err != nil {
		return fmt.Errorf("updating seeds: %w", err)
	}

	peerList := []*model.PeerSpec{}
	for _, v := range peerMap {
		peerList = append(peerList, v)
	}

	if len(peerList) == 0 {
		n.logger.Warn("no peers found")
	}

	err = n.store.UpsertPeers(peerList)
	if err != nil {
		return fmt.Errorf("updating peers: %w", err)
	}

	n.logger.Debug("joined seeds", "seeds", len(seeds), "peers", len(peerList))

	n.pingPeers()

	return nil
}

func (n *node) leaveSeeds() error {
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
	for _, seed := range seeds {
		wg.Add(1)
		go func() {
			defer wg.Done()

			n.logger.Debug("leaving seed", "seed", seed.RemoteAddr)

			ctxInner, cancelFnInner := context.WithTimeout(ctx, 5*time.Second)
			defer cancelFnInner()

			url := fmt.Sprintf("https://%s/goodbye", seed.RemoteAddr)
			req, err := http.NewRequestWithContext(ctxInner, "POST", url, nil)
			if err != nil {
				n.logger.Error("sending goodbye (constructing request)", "error", err, "remote", seed)
				return
			}

			resp, err := n.client.Do(req)
			if err != nil {
				n.logger.Error("sending goodbye", "error", err, "remote", seed)
				return
			}

			if resp.StatusCode != http.StatusAccepted {
				n.logger.Error("bad goodbye response", "remote", seed, "status", resp.StatusCode)
				return
			}
		}()
	}

	wg.Wait()

	return nil
}

func (n *node) pingPeers() error {
	n.logger.Debug("pinging peers")

	peers, err := n.store.GetAllPeers()
	if err != nil {
		return fmt.Errorf("fetching peers: %w", err)
	}

	if len(peers) == 0 {
		n.logger.Warn("no peers found")
		return nil
	}

	for _, peer := range peers {
		err := n.sendPing(peer.RemoteAddr)
		if err != nil {
			n.logger.Error("pinging peer", "error", err, "peer", peer)
			n.store.DeletePeer(peer.RemoteAddr)
		}
	}
	return nil
}

func (n *node) sendPing(remote string) error {
	n.logger.Debug("pinging peer", "remote", remote)

	ctx, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFn()

	buf := bytes.NewBufferString(n.subscriptions.String())
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://%s/ping", remote), buf)
	if err != nil {
		return fmt.Errorf("creating ping: %w", err)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending ping: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping response code: %d", resp.StatusCode)
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

func (n *node) PublishIdentity(id *identity.Identity) error {
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

	err = n.Publish(id, sb.String())
	if err != nil {
		return err
	}

	return nil
}

func (n *node) Publish(id *identity.Identity, stmt string) error {
	signer, err := identity.NewSigner(id)
	if err != nil {
		return fmt.Errorf("creating signer: %w", err)
	}

	actionID := id.Identifier + "." + model.NewID()

	signer.Add([]byte(actionID))
	signer.Add([]byte(stmt))
	encodedSig := signer.Sign()

	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	peers, err := n.store.GetAllPeers()
	if err != nil {
		return fmt.Errorf("getting peers: %w", err)
	}

	if len(peers) == 0 {
		return fmt.Errorf("no peers available")
	}

	action := Action{
		ID:               actionID,
		RemoteAddr:       n.publicAddr,
		NodeID:           n.nodeID,
		Identity:         id.Identifier,
		Timestamp:        time.Now().UTC(),
		Action:           stmt,
		ReceivedFrom:     id.Identifier+"="+encodedSig,
		EncodedSignature: encodedSig,
	}

	err = n.store.CreateAction(action)
	if err != nil {
		return fmt.Errorf("send action: saving action: %w", err)
	}

	wg := sync.WaitGroup{}
	for _, peer := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.dispatchAction(ctx, peer, action)
		}()
	}
	wg.Wait()

	return nil
}

func (n *node) dispatchAction(ctx context.Context, peer *model.PeerSpec, action Action) error {
	ctxInner, cancelFnInner := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFnInner()

	buf := bytes.NewBufferString(action.Action)

	url := fmt.Sprintf("https://%s/publish", peer.RemoteAddr)
	req, err := http.NewRequestWithContext(ctxInner, "POST", url, buf)
	req.Header.Add(HeaderIdentifier, action.Identity)
	req.Header.Add(HeaderActionID, action.ID)
	req.Header.Add(HeaderNodeID, action.NodeID)
	req.Header.Add(HeaderSignature, action.EncodedSignature)

	if len(action.ReceivedFrom) > 0 {


		signer with n.identity


		sb := strings.Builder{}
		sb.WriteString(action.ReceivedFrom)
		sb.WriteString(";")
		sb.WriteString(n.nodeID)
		sb.WriteString("=")
		sig, err := n.identity.Sign(sb.String())
		if err != nil {
			return fmt.Errorf("send action: signing received from: %w", err)
		}
		sb.WriteString(sig)
		req.Header.Add(HeaderReceivedFrom, sb.String())
	}

	if err != nil {
		return fmt.Errorf("send action: creating action request: %w", err)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("send action: executing action request: %w", err)
	}

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("send action: action request not accepted: %d", resp.StatusCode)
	}

	err = n.store.TouchPeer(peer.RemoteAddr, "")
	if err != nil {
		return fmt.Errorf("send action: touching peer: %w", err)
	}

	return nil
}

func (n *node) handleWhoIs(w http.ResponseWriter, req *http.Request) {
	id := req.PathValue("id")
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
	w.Header().Add(ContentTypeHeader, "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (n *node) handleWhoAmI(w http.ResponseWriter, req *http.Request) {
	n.logger.Info("whomai", "remote", req.RemoteAddr)
	spec := model.PeerSpec{
		CreatedAt:  time.Now().UTC(),
		RemoteAddr: n.publicAddr,
		NodeID:     n.nodeID,
	}

	data, err := json.Marshal(&spec)
	if err != nil {
		n.logger.Error("marshalling whoami", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Add(ContentTypeHeader, ContentTypeJSON)
	w.Header().Add(HeaderRemoteAddress, req.RemoteAddr)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (n *node) fetchIdentity(identifier, remoteAddr string) (*x509.Certificate, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFn()

	url := fmt.Sprintf("https://%s/whois/%s", remoteAddr, identifier)
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

func (n *node) tidyPeers() error {
	// delete any peer who hasn't been touched in the last 3 minutes
	before := time.Now().UTC().Add(-3 * time.Minute)
	err := n.store.DeleteAgedPeers(before)
	if err != nil {
		return fmt.Errorf("deleteing peers: %w", err)
	}

	return nil
}

func (n *node) CountOfPeers() (int, error) {
	return n.store.CountOfPeers()
}

func (n *node) propagateAction(action Action, entityIDs ...string) error {
	peers, err := n.store.GetAllPeers()
	if err != nil {
		return fmt.Errorf("dispatch getting peers: %w", err)
	}

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			b := bloom.New()
			err = b.Parse(p.Filter)
			if err != nil {
				n.logger.Error("dispatch parsing filter", "error", err)
				return
			}

			isWatching := false
			for _, id := range entityIDs {
				if b.Intersects([]byte(id)) {
					isWatching = true
					break
				}
			}

			if !isWatching {
				return
			}

			ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancelFn()
			n.dispatchAction(ctx, p, action)
		}()
	}
	wg.Wait()

	return nil
}

func (n *node) verifyAction(action Action) error {
	cert, err := n.store.GetCachedCertificate(action.Identity)
	if err != nil {
		if !errors.Is(err, model.ErrNotFound) {
			return fmt.Errorf("getting certificate: %w", err)
		}
		cert, err = n.fetchIdentity(action.Identity, action.RemoteAddr)
		if err != nil {
			return fmt.Errorf("fetching certificate: %w", err)
		}
	}

	v, err := identity.NewVerifier(cert)
	v.Add([]byte(action.ID))
	v.Add([]byte(action.Action))
	err = v.Verify(action.EncodedSignature)
	if err != nil {
		return err
	}

	return nil
}

func (n *node) moderateAction(action Action) error {
	//TODO: implement moderation
	return nil
}
