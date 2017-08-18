package database

type FfmpegLog struct {
	AssetId int    `gorm:"column:assetId" json:"assetId"`
	Log     string `gorm:"column:log" json:"log"`
}

func (FfmpegLog) TableName() string {
	return "ffmpegLogs"
}
