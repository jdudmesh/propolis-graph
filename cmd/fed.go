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
package cmd

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jdudmesh/propolis/internal/activitypub"
	"github.com/jdudmesh/propolis/internal/datastore"
	"github.com/spf13/cobra"
)

var fedCmd = &cobra.Command{
	Use:   "fed",
	Short: "Propolis ActivityPub integration",
	Long:  `Run an ActivityPub server`,
	Run: func(cmd *cobra.Command, args []string) {
		host, err := cmd.Flags().GetString("host")
		if err != nil {
			panic(err)
		}

		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			panic(err)
		}

		migrationsDir, err := cmd.Flags().GetString("migrations")
		if err != nil {
			panic(err)
		}

		stateStore, err := datastore.NewInternalState(migrationsDir, []string{}, []string{})
		if err != nil {
			logger.Error("store init", "error", err)
			panic("unable to init state store")
		}

		h, err := activitypub.NewServer(host, port, stateStore, logger)
		if err != nil {
			logger.Error("creating peer", "error", err)
			return
		}

		ctx, cancelFn := context.WithCancelCause(context.Background())
		defer cancelFn(errors.New("deferred"))

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := h.Run(ctx)
			if err != nil {
				logger.Error("starting ActivityPub node", "error", err)
				panic("unable to start node")
			}
		}()

		go func() {
			sigint := make(chan os.Signal, 1)
			signal.Notify(sigint, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
			for s := range sigint {
				switch s {
				case syscall.SIGHUP:
					logger.Info("sighup: reloading")
					err := h.Reload()
					if err != nil {
						logger.Error("reloading", "error", err)
					}
				case syscall.SIGINT, syscall.SIGTERM:
					cancelFn(errors.New("received term signal, exiting"))
				}
			}
		}()

		wg.Wait()
	},
}

func init() {
	baseCmd.AddCommand(fedCmd)
}
