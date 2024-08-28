package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"time"

	"github.com/jdudmesh/propolis/internal/model"
)

var (
	ErrUnsupportedPublicKey = errors.New("unsupported public key")
	ErrUnauthorized         = errors.New("unauthorized")
	ErrBadSignature         = errors.New("bad signature")
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
		Identifier: model.NewID(),
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
		ID:        model.NewID(),
		CreatedAt: id.CreatedAt,
		OwnerID:   id.Identifier,
		Type:      KeyTypeED25519PublicKey,
		Data:      publicKey,
	}

	privKeyItem := &KeyItem{
		ID:        model.NewID(),
		CreatedAt: id.CreatedAt,
		OwnerID:   id.Identifier,
		Type:      KeyTypeED25519PrivateKey,
		Data:      privateKey,
	}

	id.Keys = append(id.Keys, pubKeyItem, privKeyItem)

	return nil
}

type signer struct {
	privateKey ed25519.PrivateKey
	hash       hash.Hash
}

type verifier struct {
	publicKey ed25519.PublicKey
	hash      hash.Hash
}

func NewSigner(id *Identity) (*signer, error) {
	var privateKey ed25519.PrivateKey
	for _, key := range id.Keys {
		if key.Type == KeyTypeED25519PrivateKey {
			privateKey = key.Data
			break
		}
	}
	if privateKey == nil {
		return nil, fmt.Errorf("private key not found")
	}

	return &signer{
		privateKey: privateKey,
		hash:       sha256.New(),
	}, nil
}

func (s *signer) Add(data []byte) {
	s.hash.Write(data)
}

func (s *signer) Sign() string {
	sig := ed25519.Sign(s.privateKey, s.hash.Sum(nil))
	return base64.StdEncoding.EncodeToString(sig)
}

func NewVerifier(cert *x509.Certificate) (*verifier, error) {
	return &verifier{
		publicKey: cert.PublicKey.(ed25519.PublicKey),
		hash:      sha256.New(),
	}, nil
}

func (v *verifier) Add(data []byte) {
	v.hash.Write(data)
}

func (v *verifier) Verify(enc string) error {
	sig, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return ErrBadSignature
	}

	if !ed25519.Verify(v.publicKey, v.hash.Sum(nil), sig) {
		return ErrUnauthorized
	}

	return nil
}
