package model

type PingResponse struct {
	Address string `json:"addr"`
}

type SubscriptionRequest struct {
	Spec []string `json:"spec"`
}

type SubscriptionResponse struct {
	Peers map[string][]string `json:"peers"`
}
