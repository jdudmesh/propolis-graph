package datastore

import (
	"context"
	"fmt"
	"time"

	"github.com/jdudmesh/propolis/internal/model"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type internalStateStore struct {
	db *sqlx.DB
}

func NewInternalState(seeds []string) (*internalStateStore, error) {
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
		create table seeds (
			remote_addr text not null primary key,
			created_at datetime not null,
			updated_at datetime null
		);
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		create table peers (
			remote_addr text not null primary key,
			created_at datetime not null,
			updated_at datetime null
		);
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		create table subs (
			remote_addr text not null,
			spec text not null,
			created_at datetime not null,
			updated_at datetime null,
			primary key(remote_addr, spec),
			foreign key(remote_addr) references peers(remote_addr)
		);
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		create index idx_subscriptions_spec on subscriptions(spec);
	`)
	if err != nil {
		return err
	}

	return nil
}

func (s *internalStateStore) initSeeds(seeds []string) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second)
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

func (s *internalStateStore) GetPeers() ([]*model.PeerSpec, error) {
	rows, err := s.db.Queryx(`select * from peers`)
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

func (s *internalStateStore) GetSubs() ([]*model.SubscriptionSpec, error) {
	rows, err := s.db.Queryx(`select * from subs`)
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

func (s *internalStateStore) UpsertSubs(remoteAddr string, subs []string) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second)
	defer cancelFn()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("saving seeds (begin): %w", err)
	}

	now := time.Now().UTC()
	for _, s := range subs {
		_, err = tx.Exec(`
			insert into peers(remote_addr, created_at)
			values(?, ?)
			on conflict(remote_addr) do update set updated_at = ?`,
			remoteAddr, now, now)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("updating subs (insert peer): %w", err)
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
			return fmt.Errorf("updating subs (insert sub): %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("updating subs(commit): %w", err)
	}

	return nil
}

func (s *internalStateStore) FindPeersBySub(sub string) ([]*model.PeerSpec, error) {
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
