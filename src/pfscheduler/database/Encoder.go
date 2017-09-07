package database

import (
	"time"
)

type Encoder struct {
	ID          int       `gorm:"primary_key;column:encoderId" json:"encoderId"`
	Hostname    string    `gorm:"column:hostname" json:"hostname"`
	ActiveTasks int       `gorm:"column:activeTasks;default:0" json:"activeTasks"`
	MaxTasks    int       `gorm:"column:maxTasks;default:1" json:"maxTasks"`
	Load1       float32   `gorm:"column:load1;default:0" json:"load1"`
	UpdatedAt   time.Time `gorm:"column:updatedAt" sql:"DEFAULT:CURRENT_TIMESTAMP" json:"updatedAt"`
}

func (Encoder) TableName() string {
	return "encoders"
}
