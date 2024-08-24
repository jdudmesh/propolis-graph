package identity

import (
	"crypto/ecdsa"
	"time"
)

type Identity struct {
	Identifier     string           `db:"id"`
	CreatedAt      time.Time        `db:"created_at"`
	UpdatedAt      *time.Time       `db:"updated_at"`
	Handle         string           `db:"handle"`
	Bio            string           `db:"bio"`
	PublicKeyData  string           `db:"public_key"`
	PrivateKeyData string           `db:"private_key"`
	PublicKey      ecdsa.PublicKey  `db:"-"`
	PrivateKey     ecdsa.PrivateKey `db:"-"`
}
