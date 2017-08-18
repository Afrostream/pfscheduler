package database

type FfmpegProgressV0 struct {
	Frame   string
	Fps     string
	Q       string
	Size    string
	Elapsed string
	Bitrate string
}

type FfmpegProgress struct {
	AssetId int     `gorm:"primary_key;column:assetId" json:"assetId"` /* forcing primary_key, none yet in DB in order to prevent multiple inserts */
	Frame   int     `gorm:"column:frame" json:"frame"`
	Fps     int     `gorm:"column:fps" json:"fps"`
	Q       float32 `gorm:"column:q" json:"q"`
	Size    int     `gorm:"column:size" json:"size"`
	Elapsed string  `gorm:"column:elapsed" json:"elapsed"`
	Bitrate float32 `gorm:"column:bitrate" json:"bitrate"`
}

func (FfmpegProgress) TableName() string {
	return "ffmpegProgress"
}
