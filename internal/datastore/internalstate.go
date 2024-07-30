package datastore

import (
	"fmt"

	"github.com/jdudmesh/propolis/internal/peer"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type internalStateStore struct {
	db *sqlx.DB
}

func NewInternalState() (*internalStateStore, error) {
	db, err := sqlx.Connect("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	err = createStateSchema(db)
	if err != nil {
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	store := &internalStateStore{
		db,
	}

	return store, nil

}

func createStateSchema(db *sqlx.DB) error {
	_, err := db.Exec(`
		create table connections (
			stream_id int not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			host_addr text not null
		);
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		create table subscriptions (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			status int not null,
			cn_type int not null,
			connection_id text not null
		);
	`)
	if err != nil {
		return err
	}

	return nil
}

func (s *internalStateStore) CreatePeer(cn *peer.Connection) error {
	_, err := s.db.NamedExec(`
		INSERT INTO connections (
			stream_id,
			status,
			created_at,
			parent_id,
			account_type,
			quota,
			name,
			email,
			password,
			public_key,
			private_key,
			mfa_secret)
		VALUES (
			:stream_id,
			:status,
			:created_at,
			:parent_id,
			:account_type,
			:quota,
			:name,
			:email,
			:password,
			:public_key,
			:private_key,
			:mfa_secret)`, cn)

	return err
}

func (s *internalStateStore) RefreshPeer(cn *peer.Connection) error {
	_, err := s.db.NamedExec(`
		update connections
		set updated_at = :updated_at,
		status = :status
		where stream_id = :stream_id
`, cn)

	return err
}
