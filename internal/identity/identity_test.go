package identity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateIdentity(t *testing.T) {
	assert := assert.New(t)

	databaseUrl := "file::identity.db?mode=memory&cache=shared"
	//cwd := os.Getenv("WORKSPACE_DIR")
	//databaseUrl := fmt.Sprintf("file:%s/data/identity.db?mode=rwc&_secure_delete=true", cwd)
	store, err := NewStore(databaseUrl)
	assert.NoError(err)
	assert.NotNil(store)

	svc, err := NewService(store)
	assert.NoError(err)
	assert.NotNil(svc)

	id, err := svc.CreateIdentity("test user", "this is who I am", true)
	assert.NoError(err)
	assert.NotNil(id)
}
