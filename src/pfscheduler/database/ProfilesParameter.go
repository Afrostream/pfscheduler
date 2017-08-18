package database

type ProfilesParameter struct {
	ID        int    `gorm:"primary_key;column:profileParameterId" json:"profileParameterId"`
	ProfileId int    `gorm:"column:profileId" json:"profileId"`
	AssetId   int    `gorm:"column:assetId" json:"assetId"` /* TODO : NCO : ? Why here ? */
	Parameter string `gorm:"column:parameter" json:"parameter"`
	Value     string `gorm:"column:value" json:"value"`
	CreatedAt string `gorm:"column:createdAt" json:"createdAt"`
	UpdatedAt string `gorm:"column:updatedAt" json:"updatedAt"`
}

func (ProfilesParameter) TableName() string {
	return "profilesParameters"
}
