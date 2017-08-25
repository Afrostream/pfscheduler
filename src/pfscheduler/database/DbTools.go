package database

import (
	"database/sql"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"log"
	"pfscheduler/tools"
	"time"
)

var DbDsn string

func OpenGormDb() (db *gorm.DB) {
	for {
		//See : http://jinzhu.me/gorm/database.html#connecting-to-a-database
		db, err := gorm.Open("mysql", DbDsn+"?parseTime=true")
		if err == nil {
			db.LogMode(true)
			return db
		}
		tools.LogOnError(err, "Failed to connect to the database %s, error=%s, retrying...", DbDsn, err)
		time.Sleep(3 * time.Second)
	}
}

func OpenGormDbOnce() (db *gorm.DB, err error) {
	//See : http://jinzhu.me/gorm/database.html#connecting-to-a-database
	db, err = gorm.Open("mysql", DbDsn+"?parseTime=true")
	if err != nil {
		tools.LogOnError(err, "Failed to connect to the database %s, error=%s", DbDsn, err)
	}
	return
}

//DEPRECATED
func OpenDb() (db *sql.DB) {
	db, err := sql.Open("mysql", DbDsn)
	tools.LogOnError(err, "Cannot open database %s", DbDsn)
	err = db.Ping()
	tools.LogOnError(err, "Cannot ping database %s", DbDsn)

	return
}

//DEPRECATED
func DbSetContentState(db *sql.DB, contentId int, state string) (err error) {
	var stmt *sql.Stmt
	query := "UPDATE contents SET state=? WHERE contentId=?"
	stmt, err = db.Prepare(query)
	if err != nil {
		log.Printf("XX Cannot prepare query %s: %s", query, err)
		return
	}
	defer stmt.Close()
	_, err = stmt.Exec(state, contentId)
	if err != nil {
		log.Printf("XX Cannot execute query %s: %s", query, err)
		return
	}

	return
}
