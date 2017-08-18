package tasks

type JsonSetSubtitles struct {
	ContentId   *int    `json:"contentId"`
	Broadcaster *string `json:"broadcaster"`
	Subtitles   *[]Sub  `json:"subtitles"`
}
