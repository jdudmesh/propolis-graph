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
	"log/slog"
	"sync"

	"github.com/jdudmesh/propolis/internal/datastore"
	"github.com/jdudmesh/propolis/internal/peer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var peerCmd = &cobra.Command{
	Use:   "peer",
	Short: "Propolis peer",
	Long:  `Run propolis in peer mode`,
	Run: func(cmd *cobra.Command, args []string) {
		c := Config{}
		viper.Unmarshal(&c)

		stateStore, err := datastore.NewInternalState(c.Peers)
		if err != nil {
			return
		}

		h, err := peer.New(c.Host, c.Port, stateStore)
		if err != nil {
			slog.Error("creating peer", "error", err)
			return
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := h.Run()
			if err != nil {
				fmt.Println(err)
			}
		}()
		wg.Wait()
	},
}

func init() {
	peerCmd.Flags().Int("port", 9090, "Peer listen port")
	viper.BindPFlag("port", peerCmd.Flags().Lookup("port"))

	baseCmd.AddCommand(peerCmd)
}
