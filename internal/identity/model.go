package identity

import (
	"crypto/x509"
	"time"
)

type Identity struct {
	Identifier      string            `db:"id"`
	CreatedAt       time.Time         `db:"created_at"`
	UpdatedAt       *time.Time        `db:"updated_at"`
	Handle          string            `db:"handle"`
	Bio             string            `db:"bio"`
	CertificateData []byte            `db:"certificate"`
	IsPrimary       bool              `db:"is_primary"`
	Keys            []*KeyItem        `db:"-"`
	Certificate     *x509.Certificate `db:"-"`
}

type KeyType int

const (
	KeyTypeUnknown KeyType = iota
	KeyTypeECDSAPublicKey
	KeyTypeECDSAPrivateKey
	KeyTypeED25519PublicKey
	KeyTypeED25519PrivateKey
)

type KeyItem struct {
	ID        string     `db:"id"`
	CreatedAt time.Time  `db:"created_at"`
	UpdatedAt *time.Time `db:"updated_at"`
	OwnerID   string     `db:"owner_id"`
	Type      KeyType    `db:"key_type"`
	Data      []byte     `db:"data"`
}

func (i Identity) Sign(string) (string, error) {
	return "", nil
}
