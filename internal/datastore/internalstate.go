package datastore

// import (
// 	"fmt"

// 	"github.com/jdudmesh/propolis/internal/peer"
// 	"github.com/jmoiron/sqlx"
// 	_ "github.com/mattn/go-sqlite3"
// )

// type internalStateStore struct {
// 	db *sqlx.DB
// }

// func NewInternalState() (*internalStateStore, error) {
// 	db, err := sqlx.Connect("sqlite3", "file::memory:?cache=shared")
// 	if err != nil {
// 		return nil, fmt.Errorf("connecting to database: %w", err)
// 	}

// 	err = createStateSchema(db)
// 	if err != nil {
// 		return nil, fmt.Errorf("creating schema: %w", err)
// 	}

// 	store := &internalStateStore{
// 		db,
// 	}

// 	return store, nil

// }

// func createStateSchema(db *sqlx.DB) error {
// 	_, err := db.Exec(`
// 		create table hubs (
// 			host_addr text not null primary key,
// 			created_at datetime not null,
// 			updated_at datetime null
// 		);
// 	`)
// 	if err != nil {
// 		return err
// 	}

// 	_, err = db.Exec(`
// 		create table peers (
// 			stream_id int not null primary key,
// 			created_at datetime not null,
// 			updated_at datetime null,
// 			host_addr text not null
// 		);
// 	`)
// 	if err != nil {
// 		return err
// 	}

// 	_, err = db.Exec(`
// 		create table subscriptions (
// 			id text not null primary key,
// 			created_at datetime not null,
// 			updated_at datetime null,
// 			stream_id int not null primary key,
// 			host_addr text not null,
// 			spec text not null,
// 			foreign key(stream_id) references peers(stream_id)
// 		);
// 	`)
// 	if err != nil {
// 		return err
// 	}

// 	_, err = db.Exec(`
// 		create index idx_subscriptions_spec on subscriptions(spec);
// 	`)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (s *internalStateStore) UpsertHub(h peer.HubSpec) error {
// 	_, err := s.db.NamedExec(`
// 		insert into hubs (
// 		host_addr,
// 		created_at,
// 		updated_at)
// 		values(
// 		:host_addr,
// 		:created_at,
// 		:updated_at)
// 		on conflict(host_addr) do update set updated_at = :updated_at
// 	`, h)

// 	return err
// }

// func (s *internalStateStore) GetHubs() ([]*peer.HubSpec, error) {
// 	hubs := make([]*peer.HubSpec, 0)

// 	err := s.db.Select(&hubs, "select * from hubs")
// 	if err != nil {
// 		return nil, err
// 	}

// 	return hubs, nil
// }

// func (s *internalStateStore) UpsertPeer(p peer.PeerSpec) error {
// 	_, err := s.db.NamedExec(`
// 		insert into peers (
// 		stream_id,
// 		created_at,
// 		updated_at,
// 		host_addr)
// 		values(
// 		:stream_id,
// 		:created_at,
// 		:updated_at,
// 		:host_addr)
// 		on conflict(stream_id) do update set updated_at = :updated_at
// 	`, p)

// 	return err
// }

// func (s *internalStateStore) UpsertSubscription(sub peer.SubscriptionSpec) error {
// 	_, err := s.db.NamedExec(`
// 		insert into subscriptions (
// 		id,
// 		created_at,
// 		updated_at,
// 		stream_id,
// 		host_addr,
// 		spec)
// 		values(
// 		:id,
// 		:created_at,
// 		:updated_at,
// 		:stream_id,
// 		:host_addr,
// 		:spec)
// 		on conflict(stream_id) do update set updated_at = :updated_at
// 	`, sub)

// 	return err
// }

// func (s *internalStateStore) FindPeersBySubscription(sub string) ([]*peer.PeerSpec, error) {
// 	rows, err := s.db.Queryx(`
// 		select p.*
// 		from subscriptions s
// 		inner join peers p
// 		on s.stream_id = p.stream_id
// 		where s.spec = ?`, sub)

// 	if err != nil {
// 		return nil, fmt.Errorf("querying subs: %w", err)
// 	}
// 	defer rows.Close()

// 	peers := make([]*peer.PeerSpec, 0)
// 	for rows.Next() {
// 		s := &peer.PeerSpec{}
// 		err = rows.StructScan(s)
// 		if err != nil {
// 			return nil, fmt.Errorf("scanning peer: %w", err)
// 		}
// 		peers = append(peers, s)
// 	}

// 	return peers, nil
// }
