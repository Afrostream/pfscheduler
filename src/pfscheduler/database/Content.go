package database

type Content struct {
	ID         int    `gorm:"primary_key;column:contentId" json:"contentId"`
	Uuid       string `gorm:"column:uuid" json:"uuid"`
	Md5Hash    string `gorm:"column:md5Hash" json:"md5Hash"`
	Filename   string `gorm:"column:filename" json:"filename"`
	State      string `gorm:"column:state" json:"state"` /* initialized, scheduled, processing, packaging, ready, failed */
	Size       int64  `gorm:"column:size" json:"size"`
	Duration   string `gorm:"column:duration" json:"duration"`
	UspPackage string `gorm:"column:uspPackage" json:"uspPackage"` /* enabled, disabled */
	Drm        string `gorm:"column:drm" json:"drm"`               /* enabled, disabled */
	CreatedAt  string `gorm:"column:createdAt" json:"createdAt"`
	UpdatedAt  string `gorm:"column:updatedAt" json:"updatedAt"`
	ProfileIds []int  `gorm:"-" json:"profilesIds"`
}

func (Content) TableName() string {
	return "contents"
}
