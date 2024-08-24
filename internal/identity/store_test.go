package identity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {
	assert := assert.New(t)

	store, err := NewStore()
	assert.NoError(err)
	assert.NotNil(store)
}
