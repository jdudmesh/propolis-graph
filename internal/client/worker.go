package client

import (
	"time"

	"github.com/OneOfOne/xxhash"
)

type datastore interface {
	GetSubscriptions() []string
}

type worker struct {
	store       datastore
	hubs        []string
	connections []*hubConnection
	quit        chan struct{}
	lastSubs    uint64
}

func New(s datastore, hubs []string) (*worker, error) {
	return &worker{
		quit:  make(chan struct{}),
		hubs:  hubs,
		store: s,
	}, nil
}

func (w *worker) Run() error {
	for _, h := range w.hubs {
		cn := &hubConnection{hubAddr: h}
		w.connections = append(w.connections, cn)
	}

	w.CheckConnections()

	t := time.NewTicker(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			w.CheckConnections()
		case <-w.quit:
			return nil
		}
	}
}

func (w *worker) Close() error {
	for _, cn := range w.connections {
		cn.Disconnect()
	}
	close(w.quit)
	return nil
}

func (w *worker) CheckConnections() {
	subs := w.store.GetSubscriptions()
	hasher := xxhash.New64()
	for _, s := range subs {
		hasher.WriteString(s)
	}
	h := hasher.Sum64()
	shouldSendSubscriptions := h != w.lastSubs

	for _, cn := range w.connections {
		if cn.state.Load() == stateNotConnected {
			cn.Connect(subs)
		}
		if shouldSendSubscriptions {
			cn.SendSubscriptions(subs)
		} else {
			cn.Ping()
		}
	}
}
