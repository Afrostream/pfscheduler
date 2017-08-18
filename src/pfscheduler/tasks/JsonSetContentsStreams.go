package tasks

type JsonSetContentsStreams struct {
	ContentId *int      `json:"contentId"`
	Streams   *[]Stream `json:"streams"`
}
