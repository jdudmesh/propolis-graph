package datastore

var dummySubs = []string{
	"SUBSCRIBE 1",
	"SUBSCRIBE 2",
	"SUBSCRIBE 3",
	"SUBSCRIBE 4",
	"SUBSCRIBE 5",
}

type dummyStore struct {
	peers []string
}

func NewDummy(peers []string) (*dummyStore, error) {
	return &dummyStore{peers}, nil
}

func (d dummyStore) GetPeers() ([]string, error) {
	return d.peers, nil
}

func (d dummyStore) GetSubs() ([]string, error) {
	return []string{"SUBSCRIBE 1"}, nil
}
