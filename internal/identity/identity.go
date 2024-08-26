package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jdudmesh/propolis/internal/model"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

type identityStore interface {
	GetPrimaryIdentity() (*Identity, error)
	PutIdentity(id *Identity) error
}

type identityService struct {
	store identityStore
}

func NewService(store identityStore) (*identityService, error) {
	return &identityService{
		store: store,
	}, nil
}

func (s *identityService) GetPrimaryIdentity() (*Identity, error) {
	i, err := s.store.GetPrimaryIdentity()
	if err != nil {
		if !errors.Is(err, model.ErrNotFound) {
			return nil, fmt.Errorf("fetching default user: %w", err)
		}
		return s.CreateIdentity("unknown", "unknown user", true)
	}
	return i, nil
}

func (s *identityService) CreateIdentity(handle, bio string, isPrimary bool) (*Identity, error) {
	id := &Identity{
		Identifier: gonanoid.Must(),
		CreatedAt:  time.Now().UTC(),
		Handle:     handle,
		Bio:        bio,
		IsPrimary:  isPrimary,
		Keys:       []*KeyItem{},
	}

	err := s.createCredentials(id)
	if err != nil {
		return nil, fmt.Errorf("creating credentials: %w", err)
	}

	err = s.store.PutIdentity(id)
	if err != nil {
		return nil, fmt.Errorf("storing credentials: %w", err)
	}

	return id, nil
}

func (s *identityService) createCredentials(id *Identity) error {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generating new key: %s", err)
	}

	template := x509.Certificate{
		Subject: pkix.Name{
			CommonName: id.Identifier,
		},
		SerialNumber: big.NewInt(1),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey, privateKey)
	if err != nil {
		return fmt.Errorf("generating certificate: %w", err)
	}

	id.CertificateData = certDER

	pubKeyItem := &KeyItem{
		ID:        gonanoid.Must(),
		CreatedAt: id.CreatedAt,
		OwnerID:   id.Identifier,
		Type:      KeyTypeED25519PublicKey,
		Data:      publicKey,
	}

	privKeyItem := &KeyItem{
		ID:        gonanoid.Must(),
		CreatedAt: id.CreatedAt,
		OwnerID:   id.Identifier,
		Type:      KeyTypeED25519PrivateKey,
		Data:      privateKey,
	}

	id.Keys = append(id.Keys, pubKeyItem, privKeyItem)

	return nil
}
