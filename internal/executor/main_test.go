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
package executor

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"path"
	"testing"
)

var databaseUrl string
var logger *slog.Logger

func TestMain(m *testing.M) {
	log.Println("Setting up test environment")
	if cur := os.Getenv("WORKSPACE_DIR"); cur == "" {
		cur, err := os.Getwd()
		if err != nil {
			log.Fatalf("Failed to get current working directory: %v", err)
		}
		cur = path.Join(cur, "..", "..", "..")
		os.Setenv("WORKSPACE_DIR", cur)
		log.Println("WORKSPACE_DIR not set, defaulting to " + cur)
	}

	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger = slog.New(slog.NewTextHandler(os.Stdout, opts))

	cur := os.Getenv("WORKSPACE_DIR")
	//dbConn := "file::memory:?cache=shared"
	databaseUrl = fmt.Sprintf("file:%s/data/propolis.db?mode=rwc&_secure_delete=true", cur)

	m.Run()
}
