package tasks

import (
	"encoding/json"
)

type JsonTranscode struct {
	ContentId   *int    `json:"contentId"`
	Broadcaster *string `json:"broadcaster"`
}

func newJsonTranscodeFromBytes(data []byte) (jsonTranscode JsonTranscode, err error) {
	err = json.Unmarshal(data, &jsonTranscode)
	return
}