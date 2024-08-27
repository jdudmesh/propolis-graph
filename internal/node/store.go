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
	"github.com/golang-migrate/migrate/v4/source/reflect"

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
		LocalSubs_up        string
		Peers_up            string
		Subs_up             string
		SubsIdx1_up         string
		SubsIdx2_up         string
		Actions_up          string
		ActionsIdx1_up      string
		PendingSubs_up      string
		CertificateCache_up string
	}{
		Seeds_up: `create table seeds (
			remote_addr text not null primary key,
			created_at datetime not null,
			updated_at datetime null
		);`,

		LocalSubs_up: `create table local_subs (
			spec text not null primary key,
			created_at datetime not null,
			updated_at datetime null
		);`,

		Peers_up: `create table peers (
			remote_addr text not null primary key,
			created_at datetime not null,
			updated_at datetime null
		);`,

		Subs_up: `create table subs (
			remote_addr text not null,
			spec text not null,
			created_at datetime not null,
			updated_at datetime null,
			primary key(remote_addr, spec),
			foreign key(remote_addr) references peers(remote_addr)
		);`,

		SubsIdx1_up: `create index idx_subs_peerspec on subs(remote_addr, spec);`,
		SubsIdx2_up: `create index idx_subs_spec on subs(spec);`,

		Actions_up: `create table actions (
			id text not null primary key,
			created_at datetime not null,
			updated_at datetime null,
			action text not null,
			remote_addr text not null
		);`,

		ActionsIdx1_up: `create index idx_actions_peer on actions(remote_addr);`,

		PendingSubs_up: `create table pending_subs (
			remote_addr text not null,
			spec text not null,
			created_at datetime not null,
			updated_at datetime null,
			primary key(remote_addr, spec),
			foreign key(remote_addr) references peers(remote_addr)
		);`,

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

func (s *store) GetRandomPeers(excluding string) ([]*model.PeerSpec, error) {
	rows, err := s.db.Queryx(`select *
		from peers
		where remote_addr != ?
		order by coalesce(updated_at, created_at) limit 1000;`, excluding)

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

func (s *store) UpsertPeer(peer string) error {
	now := time.Now().UTC()
	_, err := s.db.Exec(`
	insert into peers(remote_addr, created_at)
	values(?, ?)
	on conflict(remote_addr) do update set updated_at = ?`,
		peer, now, now)

	if err != nil {
		return fmt.Errorf("upsert peer: %w", err)
	}

	return nil
}

func (s *store) UpsertPeers(peers []string) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("upsert peers (begin): %w", err)
	}

	now := time.Now().UTC()
	for _, p := range peers {
		_, err = tx.Exec(`
			insert into peers(remote_addr, created_at)
			values(?, ?)
			on conflict(remote_addr) do update set updated_at = ?`,
			p, now, now)
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

func (s *store) TouchSeed(remoteAddr string) error {
	now := time.Now().UTC()
	_, err := s.db.Exec(`update seeds set updated_at = ? where remote_addr = ?`, now, remoteAddr)
	if err != nil {
		return fmt.Errorf("touch seed: %w", err)
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

func (s *store) CountOfPeers() (int, error) {
	var count int
	err := s.db.Get(&count, `select count(*) from peers`)
	if err != nil {
		return 0, fmt.Errorf("count of peers: %w", err)
	}
	return count, nil
}
