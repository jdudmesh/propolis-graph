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

	"github.com/jdudmesh/propolis/internal/hub"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// hubCmd represents the serve command
var hubCmd = &cobra.Command{
	Use:   "hub",
	Short: "Run as propolis hub server",
	Long:  `A longer description that spans multiple lines and likely contains examples.`,
	Run: func(cmd *cobra.Command, args []string) {
		c := Config{}
		viper.Unmarshal(&c)
		fmt.Printf("port: %d\n", c.Port)
		h, err := hub.New(c.Host, c.Port)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Run()
		}()
		wg.Wait()

	},
}

func init() {
	baseCmd.AddCommand(hubCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// serveCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// serveCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
