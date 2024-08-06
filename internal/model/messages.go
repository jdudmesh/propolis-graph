package model

type PingResponse struct {
	Seeds []string `json:"seeds"`
}

type SubscriptionRequest struct {
	Spec []string `json:"spec"`
}

type SubscriptionResponse struct {
	Peers map[string][]string `json:"peers"`
}

type Action struct {
	Action string
}
