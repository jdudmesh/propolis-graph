package client

import (
	"context"
	"log/slog"
	"time"
)

type datastore interface {
	GetSubscriptions() []string
}

type worker struct {
	store       datastore
	hubs        []string
	connections []*hubConnection
	quit        chan struct{}
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
		ctx, cancelFn := context.WithTimeout(context.Background(), 100*time.Second)
		defer cancelFn()

		hub, err := Connect(ctx, h)
		if err != nil {
			slog.Error("connecting to hub", "error", err)
			continue
		}

		w.connections = append(w.connections, hub)
		w.CheckConnections()
	}

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
		cn.Close()
	}
	close(w.quit)
	return nil
}

func (w *worker) CheckConnections() {
	subs := w.store.GetSubscriptions()
	for _, c := range w.connections {
		c.Subscribe(subs)
	}
}
