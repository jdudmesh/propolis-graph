package graph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/jdudmesh/propolis/pkg/migrate/v4/source/reflect"
	"github.com/jmoiron/sqlx"
)

const defaultTimeout = 10 * time.Second

type store struct {
	db *sqlx.DB
}

func newStore(databaseURL string) (*store, error) {
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
		Nodes_up                  string
		NodeAttributes_up         string
		NodeAttributesIdx1_up     string
		NodeLabels_up             string
		NodeLabelsIdx1_up         string
		Relations_up              string
		RelationsIdx1_up          string
		RelationAttributes_up     string
		RelationAttributesIdx1_up string
		RelationLabels_up         string
		RelationLabelsIdx1_up     string
	}{
		Nodes_up: `create table nodes (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null
		);`,

		NodeAttributes_up: `create table node_attributes (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			node_id text not null,
			attr_name text not null,
			attr_value text not null,
			data_type int not null,
			foreign key(node_id) references nodes(id)
		);`,

		NodeAttributesIdx1_up: `create index idx_nodes_attributes_attr_name on node_attributes(attr_name);`,

		NodeLabels_up: `create table node_labels (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			node_id text not null,
			label text not null,
			foreign key(node_id) references nodes(id)
		);`,

		NodeLabelsIdx1_up: `create index idx_node_labels_label on node_labels(label);`,

		Relations_up: `create table relations (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			left_node_id text not null,
			right_node_id text not null,
			direction int not null,
			foreign key(left_node_id) references nodes(id),
			foreign key(right_node_id) references nodes(id)
		);`,

		RelationsIdx1_up: `create index idx_relations_direction on relations(direction);`,

		RelationAttributes_up: `create table relation_attributes (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			relation_id text not null,
			attr_name text not null,
			attr_value text not null,
			data_type int not null,
			foreign key(relation_id) references relations(id)
		);`,

		RelationAttributesIdx1_up: `create index idx_relation_attributes_attr_name on relation_attributes(attr_name);`,

		RelationLabels_up: `create table relation_labels(
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			relation_id text not null,
			label text not null,
			foreign key(relation_id) references relations(id)
		);`,

		RelationLabelsIdx1_up: `create index relation_labels_label on relation_labels(label);`,
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

func (s *store) CreateTx(ctx context.Context) (*sqlx.Tx, error) {
	return s.db.BeginTxx(ctx, nil)
}
