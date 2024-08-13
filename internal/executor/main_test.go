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
