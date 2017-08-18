package tasks

type Stream struct {
	Type    *string `json:"type"`
	Channel *int    `json:"channel"`
	Lang    *string `json:"lang"`
}
