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
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jdudmesh/propolis/internal/bloom"
	"github.com/jdudmesh/propolis/internal/graph"
	"github.com/jdudmesh/propolis/internal/node"
	"github.com/spf13/cobra"
)

var peerCmd = &cobra.Command{
	Use:   "peer",
	Short: "Propolis peer server",
	Long:  `Run propolis in peer mode`,
	RunE: func(cmd *cobra.Command, args []string) error {
		host, err := cmd.Flags().GetString("host")
		if err != nil {
			return fmt.Errorf("no host: %w", err)
		}

		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			return fmt.Errorf("no port: %w", err)
		}

		isMemory, err := cmd.Flags().GetBool("mem")
		if err != nil {
			return fmt.Errorf("no memory flag: %w", err)
		}

		var nodeDatabaseURL, graphDatabaseURL string
		if isMemory {
			nodeDatabaseURL = fmt.Sprintf("file:node%d.db?mode=memory&cache=shared&_secure_delete=true", port)
			graphDatabaseURL = fmt.Sprintf("file:graph%d.db?mode=memory&cache=shared&_secure_delete=true", port)
		} else {
			nodeDatabaseURL, err = cmd.Flags().GetString("ndb")
			if err != nil {
				return fmt.Errorf("no db: %w", err)
			}
			graphDatabaseURL, err = cmd.Flags().GetString("gdb")
			if err != nil {
				return fmt.Errorf("no db: %w", err)
			}
		}

		seeds, err := cmd.Flags().GetStringArray("seed")
		if err != nil {
			return fmt.Errorf("no seeds specified: %w", err)
		}

		config := node.Config{
			Config: graph.Config{
				Logger:           logger,
				GraphDatabaseURL: graphDatabaseURL,
			},
			Type:            node.NodeTypePeer,
			Host:            host,
			Port:            port,
			NodeDatabaseURL: nodeDatabaseURL,
			Seeds:           seeds,
		}

		filter := bloom.New()

		h, err := node.New(config, filter)
		if err != nil {
			return fmt.Errorf("creating peer: %w", err)
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = h.Run()
			if err != nil {
				err = fmt.Errorf("starting peer: %w", err)
			}
		}()

		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
		s := <-sigint
		logger.Info("stopping server", "signal", s)
		err = h.Close()
		if err != nil {
			logger.Error("shutting down main server", "error", err)
		}

		wg.Wait()

		return err
	},
}

func init() {
	baseCmd.AddCommand(peerCmd)
}
