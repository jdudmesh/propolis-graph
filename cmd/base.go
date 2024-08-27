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
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string
var logger *slog.Logger

// baseCmd represents the base command when called without any subcommands
var baseCmd = &cobra.Command{
	Use:   "propolis",
	Short: "A brief description of your application",
	Long:  `A longer description that spans multiple lines.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(l *slog.Logger) {
	logger = l // TODO: yuk, don't do this
	err := baseCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {

	baseCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./propolis.yaml)")
	baseCmd.PersistentFlags().String("host", "0.0.0.0", "Peer listen address")
	baseCmd.PersistentFlags().Int("port", 9090, "Peer listen port")
	baseCmd.PersistentFlags().String("ndb", "file:./data/node.db?mode=rwc&_secure_delete=true", "Node DB connection string")
	baseCmd.PersistentFlags().String("gdb", "file:./data/graph.db?mode=rwc&_secure_delete=true", "Graph DB connection string")
	baseCmd.PersistentFlags().StringArray("seed", []string{}, "host:port spec for seed")

	viper.BindPFlag("host", baseCmd.Flags().Lookup("host"))
	viper.BindPFlag("port", baseCmd.Flags().Lookup("port"))
	viper.BindPFlag("db", baseCmd.Flags().Lookup("db"))
	viper.BindPFlag("seed", baseCmd.Flags().Lookup("seed"))

	cobra.OnInitialize(initConfig)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetDefault("host", "0.0.0.0")
	viper.SetDefault("port", "9090")

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigType("yaml")
		viper.SetConfigName("propolis")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
