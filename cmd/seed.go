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
	"sync"

	"github.com/jdudmesh/propolis/internal/model"
	"github.com/jdudmesh/propolis/internal/node"
	"github.com/spf13/cobra"
)

var seedCmd = &cobra.Command{
	Use:   "seed",
	Short: "Propolis seed server",
	Long:  `Run propolis in seed mode`,
	RunE: func(cmd *cobra.Command, args []string) error {
		host, err := cmd.Flags().GetString("host")
		if err != nil {
			return fmt.Errorf("no host: %w", err)
		}

		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			return fmt.Errorf("no port: %w", err)
		}

		nodeDatabaseURL, err := cmd.Flags().GetString("ndb")
		if err != nil {
			return fmt.Errorf("no db: %w", err)
		}

		graphDatabaseURL, err := cmd.Flags().GetString("gdb")
		if err != nil {
			return fmt.Errorf("no db: %w", err)
		}

		seeds, err := cmd.Flags().GetStringArray("seed")
		if err != nil {
			return fmt.Errorf("no seeds specified: %w", err)
		}

		config := model.NodeConfig{
			Type:             model.NodeTypeSeed,
			Host:             host,
			Port:             port,
			Logger:           logger,
			NodeDatabaseURL:  nodeDatabaseURL,
			GraphDatabaseURL: graphDatabaseURL,
		}
		h, err := node.New(config)
		if err != nil {
			return fmt.Errorf("creating peer: %w", err)
		}

		err = h.SetInitialSeeds(seeds)
		if err != nil {
			return fmt.Errorf("setting initial seeds: %w", err)
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = h.Run()
			if err != nil {
				err = fmt.Errorf("startting peer: %w", err)
			}
		}()

		wg.Wait()

		return err
	},
}

func init() {
	baseCmd.AddCommand(seedCmd)
}
