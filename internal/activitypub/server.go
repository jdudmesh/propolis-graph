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
