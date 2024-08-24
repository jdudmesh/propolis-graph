package identity

import (
	"database/sql"
	"errors"
	"fmt"
)

type identityStore interface {
	GetDefaultIdentity() (*Identity, error)
}

type identityService struct {
	store identityStore
}

func NewUser(store identityStore) (*identityService, error) {
	return &identityService{
		store: store,
	}, nil
}

func (s *identityService) GetDefaultIdentity() (*Identity, error) {
	i, err := s.store.GetDefaultIdentity()
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("fetching default user: %w", err)
		}
		return s.CreateDefaultIdentity("unknown user")
	}
	return i, nil
}

func (s *identityService) CreateDefaultIdentity(handle string) (*Identity, error) {
}
