package database

type Preset struct {
	ID                 int    `gorm:"primary_key;column:presetId" json:"presetId"`
	ProfileId          int    `gorm:"column:profileId" json:"profileId"`
	PresetIdDependance string `gorm:"column:presetIdDependance" json:"presetIdDependance"`
	Name               string `gorm:"column:name" json:"name"`
	Type               string `gorm:"column:type" json:"type"`
	DoAnalyze          bool   `gorm:"column:doAnalyze" json:"doAnalyze"`
	CmdLine            string `gorm:"column:cmdLine" json:"cmdLine"`
	CreatedAt          string `gorm:"column:createdAt" json:"createdAt"`
	UpdatedAt          string `gorm:"column:updatedAt" json:"updatedAt"`
}

func (Preset) TableName() string {
	return "presets"
}
