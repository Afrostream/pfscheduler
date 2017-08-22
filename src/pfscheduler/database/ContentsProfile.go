package database

type ContentsProfile struct {
	ContentId int `gorm:"column:contentId"`
	ProfileId int `gorm:"column:profileId"`
}

func (ContentsProfile) TableName() string {
	return "contentsProfiles"
}
