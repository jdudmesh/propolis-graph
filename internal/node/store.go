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
	"github.com/jdudmesh/propolis/internal/model"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const defaultTimeout = 10 * time.Second

type store struct {
	db *sqlx.DB
}

func newStore(databaseURL, migrationsDir string) (*store, error) {
	db, err := sqlx.Connect("sqlite3", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	err = createSchema(db, migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	store := &store{
		db,
	}

	return store, nil
}

func createSchema(db *sqlx.DB, migrationsDir string) error {
	driver, err := sqlite3.WithInstance(db.DB, &sqlite3.Config{})
	if err != nil {
		return fmt.Errorf("creating driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://"+migrationsDir,
		"sqlite3", driver)

	if err != nil {
		return fmt.Errorf("creating migration: %w", err)
	}

	err = m.Up()
	if !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil
}

func (s *store) UpsertSeeds(seeds []string) error {
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
		_, err = tx.Exec("insert into seeds(remote_addr, created_at) values(?, ?)", s, time.Now().UTC())
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

func (s *store) InitSubs(subs []string) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("saving subs (begin): %w", err)
	}

	_, err = tx.Exec("delete from local_subs")
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			return fmt.Errorf("saving local_subs (rollback): %w", err)
		}
		return fmt.Errorf("saving local_subs (delete): %w", err)
	}

	now := time.Now().UTC()
	for _, sub := range subs {
		_, err := tx.Exec(`insert into local_subs(spec, created_at)
			values (?, ?)
			on conflict(spec) do update set updated_at = ?
			`, sub, now, now)

		if err != nil {
			err2 := tx.Rollback()
			if err2 != nil {
				return fmt.Errorf("saving local_subs (rollback): %w", err)
			}
			return fmt.Errorf("saving local_subs (insert): %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("saving local_subs (commit): %w", err)
	}

	return nil
}

func (s *store) GetSeeds() ([]*model.PeerSpec, error) {
	rows, err := s.db.Queryx(`select * from seeds`)
	if err != nil {
		return nil, fmt.Errorf("querying seeds: %w", err)
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

func (s *store) GetPeers() ([]*model.PeerSpec, error) {
	rows, err := s.db.Queryx(`select * from peers`)
	if err != nil {
		return nil, fmt.Errorf("querying peers: %w", err)
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

func (s *store) UpsertPeersForSub(sub string, peers []string) error {
	existing, err := s.FindPeersBySub(sub)
	if err != nil {
		return fmt.Errorf("upsert peers/subs (finding existing): %w", err)
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("upsert peers/subs (begin): %w", err)
	}

	current := map[string]struct{}{}
	now := time.Now().UTC()
	for _, p := range peers {
		current[p] = struct{}{}

		_, err = tx.Exec(`
			insert into peers(remote_addr, created_at)
			values(?, ?)
			on conflict(remote_addr) do update set updated_at = ?`,
			p, now, now)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("upsert peers/subs (insert peer): %w", err)
		}

		_, err = tx.Exec(`insert into subs(
			remote_addr,
			spec,
			created_at)
			values (?, ?, ?)
			on conflict(remote_addr, spec) do update set updated_at = ?
			`, p, sub, now, now)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("upsert peers/subs (insert sub): %w", err)
		}
	}

	for _, e := range existing {
		if _, ok := current[e.RemoteAddr]; !ok {
			_, err = tx.NamedExec(`delete from subs where remote_addr = :remote_addr and spec = :spec`, e)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("upsert peers/subs (delete old subs): %w", err)
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("upsert peers/subs (commit): %w", err)
	}

	return nil
}

func (s *store) GetSelfSubs() ([]*model.SubscriptionSpec, error) {
	rows, err := s.db.Queryx(`select * from local_subs`)
	if err != nil {
		return nil, fmt.Errorf("querying subs: %w", err)
	}
	defer rows.Close()

	subs := make([]*model.SubscriptionSpec, 0)
	for rows.Next() {
		s := &model.SubscriptionSpec{}
		err = rows.StructScan(s)
		if err != nil {
			return nil, fmt.Errorf("scanning subs: %w", err)
		}
		subs = append(subs, s)
	}

	return subs, nil
}

func (s *store) FindSubsByRemoteAddr(remoteAddr string) ([]*model.SubscriptionSpec, error) {
	rows, err := s.db.Queryx(`select * from subs where remote_addr = ?`, remoteAddr)

	if err != nil {
		return nil, fmt.Errorf("querying subs: %w", err)
	}
	defer rows.Close()

	subs := make([]*model.SubscriptionSpec, 0)
	for rows.Next() {
		s := &model.SubscriptionSpec{}
		err = rows.StructScan(s)
		if err != nil {
			return nil, fmt.Errorf("scanning subs: %w", err)
		}
		subs = append(subs, s)
	}

	return subs, nil
}

func (s *store) UpsertSubs(remoteAddr string, subs []string) error {
	existing, err := s.FindSubsByRemoteAddr(remoteAddr)
	if err != nil {
		return fmt.Errorf("upsert subs (finding existing): %w", err)
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("upsert subs (begin): %w", err)
	}

	current := map[string]struct{}{}
	now := time.Now().UTC()
	for _, s := range subs {
		current[s] = struct{}{}

		_, err = tx.Exec(`
			insert into peers(remote_addr, created_at)
			values(?, ?)
			on conflict(remote_addr) do update set updated_at = ?`,
			remoteAddr, now, now)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("upsert subs (insert peer): %w", err)
		}

		_, err = tx.Exec(`insert into subs(
			remote_addr,
			spec,
			created_at)
			values (?, ?, ?)
			on conflict(remote_addr, spec) do update set updated_at = ?
			`, remoteAddr, s, now, now)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("upsert subs (insert sub): %w", err)
		}
	}

	for _, e := range existing {
		if _, ok := current[e.Spec]; !ok {
			_, err = tx.NamedExec(`delete from subs where remote_addr = :remote_addr and spec = :spec`, e)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("upsert subs (delete old subs): %w", err)
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("upsert subs(commit): %w", err)
	}

	return nil
}

func (s *store) DeleteSubs(remoteAddr string, subs []string) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete subs (begin): %w", err)
	}

	for _, s := range subs {
		_, err := tx.Exec(`delete from subs where remote_addr = ? and spec = ?`, remoteAddr, s)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("delete subs (delete old subs): %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("delete subs(commit): %w", err)
	}

	return nil
}

func (s *store) FindPeersBySub(sub string) ([]*model.PeerSpec, error) {
	rows, err := s.db.Queryx(`
		select p.*
		from subs s
		inner join peers p
		on s.remote_addr = p.remote_addr
		where s.spec = ?`, sub)

	if err != nil {
		return nil, fmt.Errorf("querying subs: %w", err)
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

func (s *store) AddAction(id, action, remoteAddr string) error {
	_, err := s.db.Exec(`
		insert into actions (id, created_at, action, remote_addr)
		values(?, ?, ?, ?)
	`, id, time.Now().UTC(), action, remoteAddr)
	return err
}

func (s *store) AddPendingPeer(remoteAddr string, sub string) error {
	now := time.Now().UTC()
	_, err := s.db.Exec(`insert into pending_subs(
		remote_addr,
		spec,
		created_at)
		values (?, ?, ?)
		on conflict(remote_addr, spec) do update set updated_at = ?
		`, remoteAddr, sub, now, now)
	if err != nil {
		return fmt.Errorf("add pending_subs (insert sub): %w", err)
	}

	return nil
}

func (s *store) RemovePendingPeer(remoteAddr string, sub string) error {
	_, err := s.db.Exec(`delete from pending_subs where remote_addr = ? and spec = ?`, remoteAddr, sub)
	if err != nil {
		return fmt.Errorf("delete pending_subs: %w", err)
	}
	return nil
}

func (s *store) GetPendingPeersForSub(sub string) ([]*model.SubscriptionSpec, error) {
	rows, err := s.db.Queryx(`
		select *
		from pending_subs
		where spec = ?`, sub)

	if err != nil {
		return nil, fmt.Errorf("querying pending subs: %w", err)
	}
	defer rows.Close()

	peers := make([]*model.SubscriptionSpec, 0)
	for rows.Next() {
		res := &model.SubscriptionSpec{}
		err = rows.StructScan(res)
		if err != nil {
			return nil, fmt.Errorf("scanning peer: %w", err)
		}
		peers = append(peers, res)
	}

	return peers, nil
}

func (s *store) TouchPeer(remoteAddr string) error {
	now := time.Now().UTC()
	_, err := s.db.Exec(`update peers set updated_at = ? where remote_addr = ?`, now, remoteAddr)
	if err != nil {
		return fmt.Errorf("touch peer: %w", err)
	}
	return nil
}

func (s *store) CreateTx(ctx context.Context) (*sqlx.Tx, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("create tx: %w", err)
	}
	return tx, nil
}

func (s *store) PutCachedCertificate(cert *x509.Certificate) error {
	now := time.Now().UTC()
	_, err := s.db.Exec(`insert into certificate_cache (identifier, created_at, certificate)
		values (?, ?, ?)
		on conflict(identifier) do update
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
	err := s.db.Get(&certData, `select * from certificate_cache where identifier = ?`, identifier)
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
