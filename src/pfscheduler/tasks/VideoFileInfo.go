package tasks

import (
	"os"
)

type VideoFileInfo struct {
	Stat         os.FileInfo
	Duration     string
	Bitrate      string
	VideoStreams []VideoStream
	AudioStreams []AudioStream
}
