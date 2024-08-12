package activitypub

import "time"

type JsonLDMessage struct {
	ID      string         `json:"@id"`
	Type    string         `json:"@type"`
	Context map[string]any `json:"@context"`
}

type UserID string      // local user id e.g. 3GFQNuSg3dPqDD1emxv5bqX42oxq
type UserAddress string // possibly remote user id e.g. 3GFQNuSg3dPqDD1emxv5bqX42oxq@somewhere.com

type UserStatus int

const (
	UserStatusPending UserStatus = iota
	UserStatusActive
	UserStatusLocked
	UserStatusDeleted
)

type CreateUserParams struct {
	JsonLDMessage
	Handle   string `json:"handle"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

type User struct {
	ID             UserID     `db:"id" json:"id"`
	CreatedAt      time.Time  `db:"created_at" json:"createdAt"`
	UpdatedAt      *time.Time `db:"updated_at" json:"updatedAt"`
	LastLoggedInAt *time.Time `db:"last_login_at" json:"-"`
	LoginAttempts  int        `db:"login_attempts" json:"-"`
	Status         UserStatus `db:"status" json:"status"`
	Handle         string     `db:"handle" json:"handle"`
	Email          string     `db:"email" json:"email"`
	Profile        string     `db:"profile" json:"profile"`
	Password       string     `db:"password" json:"-"`
	PrivateKey     string     `db:"private_key" json:"-"`
	PublicKey      string     `db:"public_key" json:"publicKey"`
}
