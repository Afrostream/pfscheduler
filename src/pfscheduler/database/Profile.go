package database

import (
	"time"
)

type Profile struct {
	ID              int       `gorm:"primary_key;column:profileId" json:"profileId"`
	Name            string    `gorm:"column:name" json:"name"`
	Broadcaster     string    `gorm:"column:broadcaster" json:"broadcaster"`
	AcceptSubtitles string    `gorm:"column:acceptSubtitles" json:"acceptSubtitles"` /* yes, no */
	CreatedAt       time.Time `gorm:"column:createdAt" sql:"DEFAULT:CURRENT_TIMESTAMP" json:"createdAt"`
	UpdatedAt       time.Time `gorm:"column:updatedAt" sql:"DEFAULT:CURRENT_TIMESTAMP" json:"updatedAt"`
}

func (Profile) TableName() string {
	return "profiles"
}
