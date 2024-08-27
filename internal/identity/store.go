package identity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/reflect"
	"github.com/jdudmesh/propolis/internal/model"

	"github.com/jmoiron/sqlx"
)

const defaultTimeout = 10 * time.Second

type store struct {
	db *sqlx.DB
}

func NewStore(databaseURL string) (*store, error) {
	db, err := sqlx.Connect("sqlite3", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	err = createSchema(db)
	if err != nil {
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	s := &store{
		db: db,
	}

	return s, nil
}

func createSchema(db *sqlx.DB) error {
	driver, err := sqlite3.WithInstance(db.DB, &sqlite3.Config{})
	if err != nil {
		return fmt.Errorf("creating driver: %w", err)
	}

	schema := &struct {
		Identity_up string
		KeyStore_up string
	}{
		Identity_up: `create table identity (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			handle text not null,
			bio text not null default '',
			is_primary int not null default 0,
			certificate blob not null
		);`,

		KeyStore_up: `create table keys (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			owner_id text not null,
			key_type int not null,
			data blob not null
		);`,
	}

	source, err := reflect.New(schema)
	if err != nil {
		return fmt.Errorf("creating migration source driver: %w", err)
	}

	m, err := migrate.NewWithInstance("reflect", source, "sqlite3", driver)
	if err != nil {
		return fmt.Errorf("creating migration: %w", err)
	}

	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil
}

func (s *store) GetPrimaryIdentity() (*Identity, error) {
	id := &Identity{}
	err := s.db.Get(id, "select * from identity where is_primary = 1;")
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, model.ErrNotFound
		}
		return nil, fmt.Errorf("fetching identity: %w", err)
	}

	id.Keys = []*KeyItem{}
	err = s.db.Select(&id.Keys, "select * from keys where owner_id = ?", id.Identifier)
	if err != nil {
		return nil, fmt.Errorf("fetching keys: %w", err)
	}

	return id, nil
}

func (s *store) PutIdentity(id *Identity) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("put identity (begin): %w", err)
	}

	_, err = tx.NamedExecContext(ctx, `
		insert into identity (id, created_at, updated_at, handle, bio, is_primary, certificate)
		values (:id, :created_at, :updated_at, :handle, :bio, :is_primary, :certificate);
	`, id)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			return fmt.Errorf("put identity (rollback): %w", err2)
		}
		return fmt.Errorf("put identity (insert identity): %w", err)
	}

	for _, key := range id.Keys {
		_, err = tx.NamedExecContext(ctx, `
			insert into keys (id, created_at, updated_at, owner_id, key_type, data)
			values (:id, :created_at, :updated_at, :owner_id, :key_type, :data);
		`, key)
		if err != nil {
			err2 := tx.Rollback()
			if err2 != nil {
				return fmt.Errorf("put identity (rollback key): %w", err2)
			}
			return fmt.Errorf("put identity (insert key): %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("put identity (commit): %w", err)
	}

	return nil
}
