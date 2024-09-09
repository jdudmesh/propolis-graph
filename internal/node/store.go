/*
Copyright © 2024 John Dudmesh <john@dudmesh.co.uk>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/
package node

import (
	"context"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/jdudmesh/propolis/pkg/migrate/v4/source/reflect"

	"github.com/jdudmesh/propolis/internal/graph"
	"github.com/jdudmesh/propolis/internal/model"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
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

	store := &store{
		db,
	}

	return store, nil
}

func createSchema(db *sqlx.DB) error {
	driver, err := sqlite3.WithInstance(db.DB, &sqlite3.Config{})
	if err != nil {
		return fmt.Errorf("creating driver: %w", err)
	}

	schema := &struct {
		Seeds_up            string
		Peers_up            string
		Actions_up          string
		ActionsIdx1_up      string
		CertificateCache_up string
	}{
		Seeds_up: `create table seeds (
			remote_addr text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			node_id text not null
		);`,

		Peers_up: `create table peers (
			remote_addr text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			node_id text not null,
			filter text not null
		);`,

		Actions_up: `create table actions (
			id text not null primary key,
			timestamp datetime not null,
			action text not null,
			remote_addr text not null,
			node_id text not null,
			identity text not null,
			received_by text not null,
			encoded_sig text not null
		);`,

		ActionsIdx1_up: `create index idx_actions_peer on actions(remote_addr);`,

		CertificateCache_up: `create table certificate_cache (
				id text not null primary key,
				created_at datetime not null,
				updated_at datetime null,
				certificate blob not null
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

func (s *store) UpsertSeeds(seeds []*model.SeedSpec) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("saving seeds (begin): %w", err)
	}

	_, err = tx.Exec("delete from seeds")
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			return fmt.Errorf("saving seeds (rollback): %w", err)
		}
		return fmt.Errorf("saving seeds (delete): %w", err)
	}

	for _, s := range seeds {
		_, err = tx.NamedExec("insert into seeds(remote_addr, created_at, node_id) values(:remote_addr, :created_at, :node_id)", s)
		if err != nil {
			err2 := tx.Rollback()
			if err2 != nil {
				return fmt.Errorf("saving seeds (rollback): %w", err)
			}
			return fmt.Errorf("saving seeds (insert): %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("saving seeds (commit): %w", err)
	}

	return nil
}

func (s *store) GetSeeds() ([]*model.SeedSpec, error) {
	rows, err := s.db.Queryx(`select * from seeds`)
	if err != nil {
		return nil, fmt.Errorf("querying seeds: %w", err)
	}
	defer rows.Close()

	seeds := make([]*model.SeedSpec, 0)
	for rows.Next() {
		s := &model.SeedSpec{}
		err = rows.StructScan(s)
		if err != nil {
			return nil, fmt.Errorf("scanning peer: %w", err)
		}
		seeds = append(seeds, s)
	}

	return seeds, nil
}

func (s *store) TouchSeed(remoteAddr string) error {
	now := time.Now().UTC()
	_, err := s.db.Exec(`update seeds set updated_at = ? where remote_addr = ?`, now, remoteAddr)
	if err != nil {
		return fmt.Errorf("touch seed: %w", err)
	}
	return nil
}

func (s *store) GetAllPeers() ([]*model.PeerSpec, error) {
	rows, err := s.db.Queryx(`select *
		from peers
		order by coalesce(updated_at, created_at);`)

	if err != nil {
		return nil, fmt.Errorf("get all peers: %w", err)
	}
	defer rows.Close()

	peers := make([]*model.PeerSpec, 0)
	for rows.Next() {
		s := &model.PeerSpec{}
		err = rows.StructScan(s)
		if err != nil {
			return nil, fmt.Errorf("scanning peer: %w", err)
		}
		peers = append(peers, s)
	}

	return peers, nil
}

func (s *store) GetRandomPeers(excluding string, maxPeers int) ([]*model.PeerSpec, error) {
	rows, err := s.db.Queryx(`select *
		from peers
		where remote_addr != ?
		order by coalesce(updated_at, created_at) desc
		limit ?;`, excluding, maxPeers)

	if err != nil {
		return nil, fmt.Errorf("random peers: %w", err)
	}
	defer rows.Close()

	peers := make([]*model.PeerSpec, 0)
	for rows.Next() {
		s := &model.PeerSpec{}
		err = rows.StructScan(s)
		if err != nil {
			return nil, fmt.Errorf("scanning peer: %w", err)
		}
		peers = append(peers, s)
	}

	return peers, nil
}

func (s *store) DeletePeer(peer string) error {
	_, err := s.db.Exec(`delete from peers where remote_addr = ?`, peer)
	if err != nil {
		return fmt.Errorf("delete peer: %w", err)
	}
	return nil
}

func (s *store) DeleteAgedPeers(before time.Time) error {
	_, err := s.db.Exec(`delete from peers where coalesce(updated_at, created_at) < ?`, before)
	if err != nil {
		return fmt.Errorf("delete aged peers: %w", err)
	}
	return nil
}

func (s *store) UpsertPeer(peer model.PeerSpec) error {
	now := time.Now().UTC()
	peer.UpdatedAt = &now

	_, err := s.db.NamedExec(`
	insert into peers(remote_addr, created_at, node_id, filter)
	values(:remote_addr, :created_at, :node_id, :filter)
	on conflict(remote_addr) do update set updated_at = :updated_at
	`, peer)

	if err != nil {
		return fmt.Errorf("upsert peer: %w", err)
	}

	return nil
}

func (s *store) UpsertPeers(peers []*model.PeerSpec) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("upsert peers (begin): %w", err)
	}

	now := time.Now().UTC()
	for _, p := range peers {
		p.UpdatedAt = &now
		_, err := s.db.NamedExec(`
		insert into peers(remote_addr, created_at, node_id, filter)
		values(:remote_addr, :created_at, :node_id, :filter)
		on conflict(remote_addr) do update set updated_at = :updated_at
		`, p)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("upsert peers (insert peer): %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("upsert peers (commit): %w", err)
	}

	return nil
}

func (s *store) TouchPeer(remoteAddr, subsFilter string) error {
	var err error
	now := time.Now().UTC()

	if subsFilter == "" {
		_, err = s.db.Exec(`update peers set updated_at = ? where remote_addr = ?`, now, remoteAddr)
	} else {
		_, err = s.db.Exec(`update peers set filter = ?, updated_at = ? where remote_addr = ?`, subsFilter, now, remoteAddr)
	}

	if err != nil {
		return fmt.Errorf("touch peer: %w", err)
	}
	return nil
}

func (s *store) CountOfPeers() (int, error) {
	var count int
	err := s.db.Get(&count, `select count(*) from peers`)
	if err != nil {
		return 0, fmt.Errorf("count of peers: %w", err)
	}
	return count, nil
}

func (s *store) PutCachedCertificate(cert *x509.Certificate) error {
	now := time.Now().UTC()
	_, err := s.db.Exec(`insert into certificate_cache (id, created_at, certificate)
		values (?, ?, ?)
		on conflict(id) do update
		set updated_at = ?, certificate = ?`,
		cert.Subject.CommonName,
		now,
		cert.Raw,
		now,
		cert.Raw)
	if err != nil {
		return fmt.Errorf("put cached certificate: %w", err)
	}

	return nil
}

func (s *store) GetCachedCertificate(identifier string) (*x509.Certificate, error) {
	certData := []byte{}
	err := s.db.Get(&certData, `select certificate from certificate_cache where id = ?`, identifier)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, model.ErrNotFound
		}
		return nil, fmt.Errorf("get cached certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certData)
	if err != nil {
		return nil, fmt.Errorf("parsing certificate: %w", err)
	}

	return cert, nil
}

func (s *store) CreateAction(action graph.Action) error {
	_, err := s.db.NamedExec(`
		insert into actions (id, timestamp, action, remote_addr, node_id, identity, received_by, encoded_sig)
		values(:id, :timestamp, :action, :remote_addr, :node_id, :identity, :received_by, :encoded_sig)
	`, &action)
	return err
}

func (s *store) IsActionProcessed(id string) (bool, error) {
	var count int
	err := s.db.Get(&count, `select count(*) from actions where id = ?`, id)
	if err != nil {
		return false, fmt.Errorf("is action processed: %w", err)
	}
	return count > 0, nil
}
