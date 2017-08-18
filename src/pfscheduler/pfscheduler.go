package main

import (
	"fmt"
	"log"
	"os"
	"pfscheduler/database"
	"pfscheduler/tasks"
	"runtime"
	"strconv"
)

func main() {
	log.Println("-- pfscheduler starting...")

	initGlobals()

	initChecks()

	/* create tasks */
	schedulerExchangerTask := createSchedulerExchangerTask()
	schedulerHttpServerTask := createSchedulerHttpServerTask()

	/* all is ok, start tasks */

	schedulerExchangerTask.Start()
	schedulerHttpServerTask.Start()

	log.Println("-- pfscheduler started, To exit press CTRL+C")
	runtime.Goexit()
	log.Println("-- pfscheduler stopped")
}

func initGlobals() {
	log.Println("-- initGlobals starting...")
	mysqlHost := os.Getenv("MYSQL_HOST")
	mysqlUser := os.Getenv("MYSQL_USER")
	mysqlPassword := os.Getenv("MYSQL_PASSWORD")
	mySqlPort := 3306
	if os.Getenv("MYSQL_PORT") != "" {
		mySqlPort, _ = strconv.Atoi(os.Getenv("MYSQL_PORT"))
	}
	mySqlDatabase := "video_encoding"
	if os.Getenv("MYSQL_DATABASE") != "" {
		mySqlDatabase = os.Getenv("MYSQL_DATABASE")
	}
	database.DbDsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mysqlUser, mysqlPassword, mysqlHost, mySqlPort, mySqlDatabase)
	log.Println("-- initGlobals done successfully")
}

func initChecks() {
	log.Println("-- initChecks starting...")
	//TODO : binaries are functional
	//database check (blocked until database is started)
	db := database.OpenGormDb()
	defer db.Close()
	//rabbitMQ check (later)
	log.Println("-- initChecks done successfully")
}

func createSchedulerExchangerTask() tasks.SchedulerExchangerTask {
	log.Println("-- SchedulerExchangerTask calling...")
	rabbitmqHost := os.Getenv("RABBITMQ_HOST")
	rabbitmqUser := os.Getenv("RABBITMQ_USER")
	rabbitmqPassword := os.Getenv("RABBITMQ_PASSWORD")
	rabbitmqPort := 5672
	if os.Getenv("RABBITMQ_PORT") != "" {
		rabbitmqPort, _ = strconv.Atoi(os.Getenv("RABBITMQ_PORT"))
	}
	schedulerExchangerTask := tasks.NewSchedulerExchangerTask(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword)
	schedulerExchangerTask.Init()
	log.Println("-- SchedulerExchangerTask calling done successfully")
	return schedulerExchangerTask
}

func createSchedulerHttpServerTask() tasks.SchedulerHttpServerTask {
	log.Println("-- SchedulerHttpServerTask calling...")
	schedulerHttpServerTask := tasks.NewSchedulerHttpServerTask()
	schedulerHttpServerTask.Init()
	log.Println("-- SchedulerHttpServerTask calling done successfully")
	return schedulerHttpServerTask
}
