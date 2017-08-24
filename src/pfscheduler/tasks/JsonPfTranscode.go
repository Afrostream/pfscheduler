package tasks

import (
	"encoding/json"
)

type JsonPfTranscode struct {
	ContentId   int    `json:"contentId"`
	Broadcaster string `json:"broadcaster"`
}

func newJsonPfTranscodeFromBytes(data []byte) (jsonPfTranscode JsonPfTranscode, err error) {
	err = json.Unmarshal(data, &jsonPfTranscode)
	return
}