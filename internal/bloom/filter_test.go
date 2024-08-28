package bloom

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	assert := assert.New(t)

	f := New()
	f.Set([]byte("hello"))
	assert.True(f.Intersects([]byte("hello")))
	assert.False(f.Intersects([]byte("world")))

	v := f.String()
	assert.NotEmpty(v)

	f2 := New()
	err := f2.Parse(v)
	assert.NoError(err)
	assert.True(f2.Intersects([]byte("hello")))
}
