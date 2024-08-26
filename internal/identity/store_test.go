package identity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {
	assert := assert.New(t)

	databaseUrl := "file::memory:?cache=shared"
	store, err := NewStore(databaseUrl)
	assert.NoError(err)
	assert.NotNil(store)
}
