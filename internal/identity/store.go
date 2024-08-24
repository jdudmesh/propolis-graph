package identity

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/sqlite3"
	"github.com/jdudmesh/migrate/source/reflect"
	"github.com/jmoiron/sqlx"
)

type store struct {
	db *sqlx.DB
}

const usersUp = ``
const usersDown = ``

func NewStore() (*store, error) {
	db, err := sqlx.Connect("sqlite3", "file:identity.db")
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

	s.init()

	return s, nil
}

func createSchema(db *sqlx.DB) error {
	driver, err := sqlite3.WithInstance(db.DB, &sqlite3.Config{})
	if err != nil {
		return fmt.Errorf("creating driver: %w", err)
	}

	schema := &struct {
		Identity_up string
	}{
		Identity_up: `create table identity (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			public_key text not null,
			private_key text not null,
			handle text not null,
			bio text not null default '',
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
	if !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil
}

func (s *store) init() {

}
