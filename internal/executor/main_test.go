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
	"log"
	"os"
	"path"
	"testing"
)

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
	m.Run()
}
