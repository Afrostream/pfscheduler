package database

type Encoder struct {
	ID          int     `gorm:"primary_key;column:encoderId" json:"encoderId"`
	Hostname    string  `gorm:"column:hostname" json:"hostname"`
	ActiveTasks int     `gorm:"column:activeTasks;default:0" json:"activeTasks"`
	MaxTasks    int     `gorm:"column:maxTasks;default:1" json:"maxTasks"`
	Load1       float32 `gorm:"column:load1;default:1" json:"load1"`
	UpdatedAt   string  `gorm:"column:updatedAt;default:CURRENT_TIMESTAMP" json:"updatedAt"`
}

func (Encoder) TableName() string {
  return "encoders"
}