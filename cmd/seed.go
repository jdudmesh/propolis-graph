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
	"sync"

	"github.com/jdudmesh/propolis/internal/datastore"
	"github.com/jdudmesh/propolis/internal/node"
	"github.com/spf13/cobra"
)

var seedCmd = &cobra.Command{
	Use:   "seed",
	Short: "Propolis Seed",
	Long:  `Run propolis in seed mode`,
	Run: func(cmd *cobra.Command, args []string) {
		host, err := cmd.Flags().GetString("host")
		if err != nil {
			panic(err)
		}

		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			panic(err)
		}

		seeds, err := cmd.Flags().GetStringArray("seed")
		if err != nil {
			panic(err)
		}

		stateStore, err := datastore.NewInternalState(seeds, []string{})
		if err != nil {
			logger.Error("store init", "error", err)
			panic("unable to init state store")
		}

		h, err := node.NewSeed(host, port, stateStore, logger)
		if err != nil {
			logger.Error("creating peer", "error", err)
			return
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := h.Run()
			if err != nil {
				logger.Error("starting peer", "error", err)
				panic("unable to start peer")
			}
		}()
		wg.Wait()
	},
}

func init() {
	baseCmd.AddCommand(seedCmd)
}
