package tasks

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"pfscheduler/database"
	"pfscheduler/tools"
	"strconv"
	"strings"
	"time"
)

type SchedulerExchangerTask struct {
	/* constructor */
	rabbitmqHost     string
	rabbitmqPort     int
	rabbitmqUser     string
	rabbitmqPassword string
	/* 'instance' variables */
	amqpUri        string
	initialized    bool
	currentChannel *amqp.Channel
	currentQueue   amqp.Queue
	/* For test purpose only **/
	transcodingVersion int
}

func NewSchedulerExchangerTask(rabbitmqHost string, rabbitmqPort int, rabbitmqUser string, rabbitmqPassword string) SchedulerExchangerTask {
	return (SchedulerExchangerTask{rabbitmqHost: rabbitmqHost,
		rabbitmqPort:     rabbitmqPort,
		rabbitmqUser:     rabbitmqUser,
		rabbitmqPassword: rabbitmqPassword})
}

func (e *SchedulerExchangerTask) Init() bool {
	log.Printf("-- SchedulerExchangerTask init starting...")
	if e.rabbitmqUser != "" {
		e.amqpUri = fmt.Sprintf("amqp://%s:%s@%s:%d", e.rabbitmqUser, e.rabbitmqPassword, e.rabbitmqHost, e.rabbitmqPort)
	} else {
		e.amqpUri = fmt.Sprintf("amqp://%s:%d", e.rabbitmqHost, e.rabbitmqPort)
	}
	if os.Getenv("TRANSCODING_VERSION") != "" {
		e.transcodingVersion, _ = strconv.Atoi(os.Getenv("TRANSCODING_VERSION"))
	}
	log.Printf("-- SchedulerExchangerTask init : TRANSCODING_VERSION=%d", e.transcodingVersion)
	e.initialized = true
	log.Printf("-- SchedulerExchangerTask init done successfully")
	return e.initialized
}

func (e *SchedulerExchangerTask) Start() {
	if e.initialized == false {
		log.Printf("SchedulerExchangerTask not initialized, Thread cannot start...")
		return
	}
	log.Printf("-- SchedulerExchangerTask Thread starting...")
	go e.sendEncodingTasks()
	go func() {
		for { //reconnection loop
			log.Printf("-- SchedulerExchangerTask : (Re)Connection to RabbitMQ...")
			done := false
			var err error
			/* setup */
			conn := e.connectToRabbitMQ()
			defer conn.Close()
			notify := conn.NotifyClose(make(chan *amqp.Error))
			log.Printf("-- SchedulerExchangerTask : opening channel...")
			//ch, err := conn.Channel()
			e.currentChannel, err = conn.Channel()
			tools.LogOnError(err, "SchedulerExchangerTask : Failed to open a channel")
			if err == nil {
				defer e.currentChannel.Close()
				log.Printf("-- SchedulerExchangerTask : opening channel done successfully")
			}
			if err == nil {
				log.Printf("-- SchedulerExchangerTask : declaring an exchange...")
				err = e.currentChannel.ExchangeDeclare(
					"afsm-encoders", // name
					"fanout",        // type
					true,            // durable
					false,           // auto-deleted
					false,           // internal
					false,           // no-wait
					nil,             // arguments
				)
				tools.LogOnError(err, "SchedulerExchangerTask : Failed to declare an exchange")
				if err == nil {
					log.Printf("-- SchedulerExchangerTask : declaring an exchange done successfully")
				}
			}
			//var q amqp.Queue
			if err == nil {
				log.Printf("-- SchedulerExchangerTask : declaring a queue...")
				e.currentQueue, err = e.currentChannel.QueueDeclare(
					"",
					false,
					false,
					true,
					false,
					nil,
				)
				tools.LogOnError(err, "SchedulerExchangerTask : Failed to declare a queue")
				if err == nil {
					log.Printf("-- SchedulerExchangerTask : declaring an queue done successfully")
				}
			}
			if err == nil {
				log.Printf("-- SchedulerExchangerTask : binding a queue...")
				err = e.currentChannel.QueueBind(
					e.currentQueue.Name, // queue name
					"",                  // routing key
					"afsm-encoders",     // exchange
					false,
					nil,
				)
				tools.LogOnError(err, "SchedulerExchangerTask : Failed to bind a queue")
				if err == nil {
					log.Printf("-- SchedulerExchangerTask : binding a queue done successfully")
				}
			}
			if err == nil {
				done = true
			}
			if done {
				log.Printf("-- SchedulerExchangerTask : (Re)Connection to RabbitMQ done successfully")
			} else {
				log.Printf("SchedulerExchangerTask : (Re)Connection to RabbitMQ failed")
			}
		MSGSLOOP:
			for {
				select {
				case failedError := <-notify:
					//work with error
					tools.LogOnError(failedError, "SchedulerExchangerTask : Lost connection to the RabbitMQ, will retry connection...")
					break MSGSLOOP //reconnect
					//case msg := <-msgs:
					//work with message
					//log.Printf("-- SchedulerExchangerTask : Receiving a message...")
					//log.Printf("-- SchedulerExchangerTask : Received a message: %s", msg.Body)
					//
					//log.Printf("-- SchedulerExchangerTask : Receiving a message done successfully")
				}
			}
		}
		log.Printf("SchedulerExchangerTask Thread stopped")
	}()
}

func (e *SchedulerExchangerTask) connectToRabbitMQ() *amqp.Connection {
	for {
		conn, err := amqp.Dial(e.amqpUri)
		if err == nil {
			return conn
		}
		tools.LogOnError(err, "SchedulerExchangerTask : Failed to connect to the RabbitMQ, retrying...")
		time.Sleep(3 * time.Second)
	}
}

func (e *SchedulerExchangerTask) sendEncodingTasks() {
	if e.transcodingVersion == 1 {
		e.sendEncodingTasks1()
	} else {
		e.sendEncodingTasks0()
	}
}

func (e *SchedulerExchangerTask) sendEncodingTasks1() {
	ticker := time.NewTicker(time.Second * 1)
	log.Printf("-- encoding tasks sender : thread starting...")
	go func() {
		for _ = range ticker.C {
			func() {
				if e.currentChannel == nil {
					log.Printf("-- encoding tasks sender : looping continued (currentChannel is nil)")
					return
				}
				db := database.OpenGormDb()
				defer db.Close()
				var scheduledAssets []*database.Asset
				db.Where(database.Asset{State: "scheduled"}).Find(&scheduledAssets)
				for _, scheduledAsset := range scheduledAssets {
					assetOk := true
					if scheduledAsset.AssetIdDependance != nil {
						values := strings.Split(*scheduledAsset.AssetIdDependance, ",")
						if len(values) > 0 {
							for _, value := range values {
								dependanceAssetIdStr := value
								dependanceAssetId, err := strconv.Atoi(dependanceAssetIdStr)
								if err != nil {
									log.Printf("encoding tasks sender : scheduledAssetId=%d, cannot convert AssetIdDependance=%s, error=%s", scheduledAsset.ID, dependanceAssetIdStr, err)
									assetOk = false
									break
								}
								var dependanceAsset database.Asset
								if db.Where(database.Asset{ID: dependanceAssetId}).First(&dependanceAsset).RecordNotFound() {
									log.Printf("-- encoding tasks sender : scheduledAssetId=%d, cannot find dependanceAsset with ID=%d", scheduledAsset.ID, dependanceAssetId)
									assetOk = false
									break
								}
								log.Printf("-- encoding tasks sender : scheduledAssetId=%d, dependanceAssetId=%d, dependanceAssetState=%s", scheduledAsset.ID, dependanceAsset.ID, dependanceAsset.State)
								if dependanceAsset.State != "ready" {
									assetOk = false
									break
								}
							}
						} else {
							log.Printf("encoding tasks sender : scheduledAssetId=%d, cannot split AssetIdDependance=%s", scheduledAsset.ID, scheduledAsset.AssetIdDependance)
							continue
						}
					} else {
						log.Printf("-- encoding tasks sender : scheduledAssetId=%d, no AssetIdDependance set", scheduledAsset.ID)
					}
					if assetOk {
						var encoder database.Encoder
						if db.Where("activeTasks < maxTasks AND TIMESTAMPDIFF(SECOND, updatedAt, CURRENT_TIMESTAMP) <= 1").Order("load1 ASC").First(&encoder).RecordNotFound() {
							log.Printf("-- encoding tasks sender : all encoders are full, waiting...")
							continue
						}
						//OK : asset READY, encoder READY
						log.Printf("-- Encoder '%s' will take the task assetId %d", encoder.Hostname, scheduledAsset.ID)
						body := fmt.Sprintf(`{ "hostname": "%s", "assetId": %d }`, encoder.Hostname, scheduledAsset.ID)
						e.publishExchange(body)
						//NCO : HACK (so pfencoder has time to update its activeTasks + load1)
						time.Sleep(1 * time.Second)
						//
						var content database.Content
						if db.Where(database.Content{ID: scheduledAsset.ContentId}).First(&content).RecordNotFound() {
							log.Printf("encoding tasks sender : cannot find content with ID=%", scheduledAsset.ContentId)
							continue
						}
						e.setContentState(&content, "processing")
					}
				}
				e.resetStateContents()
			}()
		}
	}()
	log.Printf("encoding tasks sender : thread stopped")
}

func (e *SchedulerExchangerTask) sendEncodingTasks0() {
	ticker := time.NewTicker(time.Second * 1)
	log.Printf("-- encoding tasks sender : thread starting...")
	go func() {
		for _ = range ticker.C {
			if e.currentChannel == nil {
				log.Printf("-- encoding tasks sender : looping continued (currentChannel is nil)")
				continue
			}
			//log.Printf("-- encoding tasks sender thread looping...")
			// Send encoding task to the queue
			db := database.OpenDb()
			query := "SELECT assetId,contentId,assetIdDependance FROM assets WHERE state='scheduled'"
			stmt, err := db.Prepare(query)
			if err != nil {
				log.Printf("XX Cannot prepare query %s: %s", query, err)
				db.Close()
				continue
			}
			defer stmt.Close()
			rows, err := stmt.Query()
			if err != nil {
				db.Close()
				continue
			}
			defer rows.Close()
			var assetIds []int
			contentIdsMap := make(map[int]bool)
			for rows.Next() {
				var assetId int
				var contentId int
				var assetIdDependance *string
				err = rows.Scan(&assetId, &contentId, &assetIdDependance)
				var assetOk = true
				if assetIdDependance != nil {
					assetIdsDependance := strings.Split(*assetIdDependance, ",")
					for _, a := range assetIdsDependance {
						query := "SELECT state FROM assets WHERE assetId=?"
						stmt, err := db.Prepare(query)
						if err != nil {
							log.Printf("XX Cannot prepare query %s: %s", query, err)
							stmt.Close()
							continue
						}
						var state string
						num, err := strconv.Atoi(a)
						if err != nil {
							log.Printf("XX Cannot strconv %s to int: %s", a, err)
							stmt.Close()
							continue
						}
						err = stmt.QueryRow(num).Scan(&state)
						if err != nil {
							log.Printf("XX Cannot query row %d with query %s: %s", *assetIdDependance, query, err)
							stmt.Close()
							continue
						}
						if state != `ready` {
							assetOk = false
							stmt.Close()
							break
						}
						stmt.Close()
					}
					if assetOk == true {
						assetIds = append(assetIds, assetId)
						contentIdsMap[contentId] = true
					}
				} else {
					assetIds = append(assetIds, assetId)
					contentIdsMap[contentId] = true
				}
			}

			for _, assetId := range assetIds {
				query := "SELECT hostname FROM encoders WHERE activeTasks < maxTasks GROUP BY load1 DESC LIMIT 1"
				stmt, err = db.Prepare(query)
				if err != nil {
					log.Printf("XX Cannot prepare query %s: %s", query, err)
					continue
				}
				defer stmt.Close()
				var hostname string
				err = stmt.QueryRow().Scan(&hostname)
				if err != nil {
					// No more encoders slots available, continue
				} else {
					query := "UPDATE encoders SET activeTasks=activeTasks+1 WHERE hostname=?"
					stmt, err = db.Prepare(query)
					if err != nil {
						log.Printf("XX Cannot prepare query %s: %s", query, err)
						continue
					}
					_, err = stmt.Exec(hostname)
					if err != nil {
						log.Printf("XX Cannot Execute query %s with %s: %s", query, hostname, err)
						continue
					}
					log.Printf("-- Encoder '%s' will take the task assetId %d", hostname, assetId)
					body := fmt.Sprintf(`{ "hostname": "%s", "assetId": %d }`, hostname, assetId)
					e.publishExchange(body)
				}
			}

			for contentId, _ := range contentIdsMap {
				database.DbSetContentState(db, contentId, "processing")
			}

			//query = "UPDATE contents SET state='packaging' WHERE contentId NOT IN (SELECT contentId FROM assets WHERE state <> 'ready') AND contents.uspPackage='enabled'"
			query = "UPDATE contents SET state='ready' WHERE contentId NOT IN (SELECT contentId FROM assets WHERE state <> 'ready')"
			stmt, err = db.Prepare(query)
			if err != nil {
				log.Printf("XX Cannot prepare query %s: %s", query, err)
				continue
			}
			defer stmt.Close()
			_, err = stmt.Exec()
			if err != nil {
				log.Printf("XX Cannot exec query %s: %s", query, err)
				continue
			}

			query = "UPDATE contents SET state='failed' WHERE contentId IN (SELECT contentId FROM assets WHERE state = 'failed')"
			stmt, err = db.Prepare(query)
			if err != nil {
				log.Printf("XX Cannot prepare query %s: %s", query, err)
				continue
			}
			defer stmt.Close()
			_, err = stmt.Exec()
			if err != nil {
				log.Printf("XX Cannot exec query %s: %s", query, err)
				continue
			}

			query = "SELECT uuid,contentId FROM contents WHERE state='packaging'"
			stmt, err = db.Prepare(query)
			if err != nil {
				log.Printf("XX Cannot prepare query %s: %s", query, err)
				continue
			}
			defer stmt.Close()
			rows, err = stmt.Query()
			if err != nil {
				log.Printf("XX Cannot prepare query %s: %s", query, err)
				continue
			}
			defer rows.Close()
			var contentUuids []ContentsUuid
			for rows.Next() {
				var cu ContentsUuid
				err = rows.Scan(&cu.Uuid, &cu.ContentId)
				if err != nil {
					log.Printf("XX Cannot scan rows for query %s: %s", query, err)
					continue
				}
				contentUuids = append(contentUuids, cu)
			}
			//NCO : only if necessary
			if contentUuids != nil {
				go packageContents(contentUuids)
			}

			db.Close()
			//log.Printf("-- encoding tasks sender thread looping done successfully")
		}
	}()
}

func (e *SchedulerExchangerTask) publishExchange(msg string) (err error) {
	log.Printf("-- SchedulerExchangerTask : Sending message '%s' on afsm-encoders queue...", msg)
	err = e.currentChannel.Publish(
		"afsm-encoders",     // exchange
		e.currentQueue.Name, // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		})
	if err == nil {
		log.Printf("-- SchedulerExchangerTask : Sending message '%s' on afsm-encoders queue done successfully", msg)
	} else {
		log.Printf("SchedulerExchangerTask : Sending message '%s' on afsm-encoders queue failed, error=%s", msg, err)
	}
	return
}

func (e *SchedulerExchangerTask) setContentState(content *database.Content, state string) {
	db := database.OpenGormDb()
	defer db.Close()
	content.State = state
	db.Save(content)
}

func (e *SchedulerExchangerTask) resetStateContents() {
	db := database.OpenGormDb()
	defer db.Close()
	//SET CONTENTS NOT READY TO READY IF ALL ASSETS ARE READY
	db.Exec("UPDATE contents SET state = 'ready' WHERE state <> 'ready' AND contentId NOT IN (SELECT contentId FROM assets WHERE state <> 'ready')")
	//SET CONTENTS NOT FAILED TO FAILED IF ANY ASSET IS FAILED
	db.Exec("UPDATE contents SET state = 'failed' WHERE state <> 'failed' AND contentId IN (SELECT contentId FROM assets WHERE state = 'failed')")
}
