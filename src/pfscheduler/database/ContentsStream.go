package database

type ContentsStream struct {
	ID           int    `gorm:"primary_key;column:contentsStreamId" json:"contentsStreamId"`
	ContentId    int    `gorm:"column:contentId" json:"contentId"`
	MapId        int    `gorm:"column:mapId" json:"mapId"`
	Type         string `gorm:"column:type" json:"type"`
	Language     string `gorm:"column:language" json:"language"`
	Codec        string `gorm:"column:codec" json:"codec"`
	CodecInfo    string `gorm:"column:codecInfo" json:"codecInfo"`
	CodecProfile string `gorm:"column:codecProfile" json:"codecProfile"`
	Bitrate      int    `gorm:"column:bitrate" json:"bitrate"`
	Frequency    int    `gorm:"column:frequency" json:"frequency"`
	Width        int    `gorm:"column:width" json:"width"`
	Height       int    `gorm:"column:height" json:"height"`
	Fps          int    `gorm:"column:fps" json:"fps"`
	CreatedAt    string `gorm:"column:createdAt" json:"createdAt"`
	UpdatedAt    string `gorm:"column:updatedAt" json:"updatedAt"`
}

func (ContentsStream) TableName() string {
	return "contentsStreams"
}
