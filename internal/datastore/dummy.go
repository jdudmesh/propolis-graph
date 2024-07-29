package datastore

var dummySubs = []string{
	"SUBSCRIBE 1",
	"SUBSCRIBE 2",
	"SUBSCRIBE 3",
	"SUBSCRIBE 4",
	"SUBSCRIBE 5",
}

type dummyStore struct {
}

func NewDummy() *dummyStore {
	return &dummyStore{}
}

func (d dummyStore) GetHubs() []string {
	return []string{"127.0.0.1:9090"}
}

func (d dummyStore) GetSubscriptions() []string {
	return []string{"SUBSCRIBE 1"}
}
