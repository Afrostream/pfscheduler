package database

import (
	"time"
)

type Preset struct {
	ID                 int       `gorm:"primary_key;column:presetId" json:"presetId"`
	ProfileId          int       `gorm:"column:profileId" json:"profileId"`
	PresetIdDependance string    `gorm:"column:presetIdDependance" json:"presetIdDependance"`
	Name               string    `gorm:"column:name" json:"name"`
	Type               string    `gorm:"column:type" json:"type"`
	DoAnalyze          string    `gorm:"column:doAnalyze" json:"doAnalyze"`
	CmdLine            string    `gorm:"column:cmdLine" json:"cmdLine"`
	CreatedAt          time.Time `gorm:"column:createdAt" sql:"DEFAULT:CURRENT_TIMESTAMP" json:"createdAt"`
	UpdatedAt          time.Time `gorm:"column:updatedAt" sql:"DEFAULT:CURRENT_TIMESTAMP" json:"updatedAt"`
}

func (Preset) TableName() string {
	return "presets"
}
