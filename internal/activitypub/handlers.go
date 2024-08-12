package activitypub

import "net/http"

func webfingerHandler(w http.ResponseWriter, r *http.Request)    {}
func globalInboxHandler(w http.ResponseWriter, r *http.Request)  {}
func userInboxHandler(w http.ResponseWriter, r *http.Request)    {}
func userInfoHandler(w http.ResponseWriter, r *http.Request)     {}
func globalOutboxHandler(w http.ResponseWriter, r *http.Request) {}
func userOutboxHandler(w http.ResponseWriter, r *http.Request)   {}
