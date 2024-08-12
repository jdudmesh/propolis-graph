package activitypub

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
)

type store interface{}

type server struct {
	host       string
	port       int
	db         store
	logger     *slog.Logger
	httpServer http.Server
}

func NewServer(host string, port int, db store, logger *slog.Logger) (*server, error) {
	return &server{
		host:   host,
		port:   port,
		db:     db,
		logger: logger,
	}, nil
}

func (s *server) Run(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	mux := newmux()

	srv := http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		s.logger.Info("starting activitypub server")
		err := srv.ListenAndServe()
		if err != nil {
			s.logger.Error("fed server", "error", err)
		}
	}()

	<-ctx.Done()
	srv.Close()

	return nil
}

func (s *server) Reload() error {
	return nil
}
