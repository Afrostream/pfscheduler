package tasks

import (
	"encoding/json"
)

type JsonTranscode struct {
	Uuid      string `json:"uuid"`
	Md5Hash   string `json:"md5Hash"`
	ProfileId int    `json:"profileId"`
}

func newJsonTranscodeFromBytes(data []byte) (jsonTranscode JsonTranscode, err error) {
	err = json.Unmarshal(data, &jsonTranscode)
	return
}
