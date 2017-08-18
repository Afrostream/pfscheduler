package database

type Profile struct {
	ID              int    `gorm:"primary_key;column:profileId" json:"profileId"`
	Name            string `gorm:"column:name" json:"name"`
	Broadcaster     string `gorm:"column:broadcaster" json:"broadcaster"`
	AcceptSubtitles string `gorm:"column:acceptSubtitles" json:"acceptSubtitles"` /* yes, no */
	CreatedAt       string `gorm:"column:createdAt" json:"createdAt"`
	UpdatedAt       string `gorm:"column:updatedAt" json:"updatedAt"`
}

func (Profile) TableName() string {
	return "profiles"
}
