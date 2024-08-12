package activitypub

import "net/http"

func newmux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/.well-known/webfinger", webfingerHandler)
	mux.HandleFunc("/inbox", globalInboxHandler)
	mux.HandleFunc("/inbox/{username}", userInboxHandler)
	mux.HandleFunc("/user/{username}", userInfoHandler)
	mux.HandleFunc("/outbox", globalOutboxHandler)
	mux.HandleFunc("/outbox/{username}", userOutboxHandler)

	return mux
}
