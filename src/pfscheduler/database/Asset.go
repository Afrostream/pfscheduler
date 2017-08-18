package database

type Asset struct {
	ID                int    `gorm:"primary_key;column:assetId" json:"assetId"`
	ContentId         int    `gorm:"column:contentId" json:"contentId"`
	PresetId          int    `gorm:"column:presetId" json:"presetId"`
	AssetIdDependance string `gorm:"column:assetIdDependance" json:"assetIdDependance"`
	Filename          string `gorm:"column:filename" json:"filename"`
	DoAnalyze         string `gorm:"column:doAnalyze" json:"doAnalyze"` /* yes, no */
	State             string `gorm:"column:state" json:"state"`         /* scheduled, processing, ready failed */
	CreatedAt         string `gorm:"column:createdAt" json:"createdAt"`
	UpdatedAt         string `gorm:"column:updatedAt" json:"updatedAt"`
}

func (Asset) TableName() string {
	return "assets"
}
