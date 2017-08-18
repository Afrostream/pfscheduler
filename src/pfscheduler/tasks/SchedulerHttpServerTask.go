package tasks

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"pfscheduler/database"
	"pfscheduler/tools"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var ffmpegPath = os.Getenv("FFMPEG_PATH")
var uspPackagePath = os.Getenv("USP_PACKAGE_PATH")

var encodedBasePath = os.Getenv("VIDEOS_ENCODED_BASE_PATH")

type SchedulerHttpServerTask struct {
	/* 'instance' variables */
	router      *mux.Router
	initialized bool
}

func NewSchedulerHttpServerTask() SchedulerHttpServerTask {
	return (SchedulerHttpServerTask{})
}

func (h *SchedulerHttpServerTask) Init() bool {
	log.Printf("-- SchedulerHttpServerTask init starting...")
	//
	h.router = mux.NewRouter()
	h.router.HandleFunc("/{a:.*}", h.optionsGetHandler).Methods("OPTIONS")
	h.router.HandleFunc("/api/contents", h.contentsGetHandler).Methods("GET").Queries("state", "{state:(?:initialized|scheduled|processing|failed|ready)}", "uuid", "{uuid:[0-9a-fA-F\\-]+}")
	h.router.HandleFunc("/api/contents", h.contentsGetHandler).Methods("GET").Queries("uuid", "{uuid:[0-9a-fA-F\\-]+}")
	h.router.HandleFunc("/api/contents", h.contentsGetHandler).Methods("GET").Queries("state", "{state:(?:initialized|scheduled|processing|failed|ready)}")
	h.router.HandleFunc("/api/contents", h.contentsGetHandler).Methods("GET").Queries("md5Hash", "{md5Hash:[0-9a-f\\-]+}")
	h.router.HandleFunc("/api/contents", h.contentsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/contents", h.contentsPostHandler).Methods("POST")
	h.router.HandleFunc("/api/contents/{id:[0-9]+}", h.contentsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/contents/{id:[0-9a-z\\-]*}/contentsStreams", h.contentsStreamsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/contents/{contentId:[0-9a-z\\-]*}/assetsStreams", h.assetsStreamsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/contents/{contentId:[0-9]+}/assets", h.assetsGetHandler).Methods("GET").Queries("profileName", "{profileName:.*}", "broadcaster", "{broadcaster:.*}", "presetsType", "{presetsType:.*}")
	h.router.HandleFunc("/api/contents/{contentId:[0-9]+}/assets", h.assetsGetHandler).Methods("GET").Queries("profileName", "{profileName:.*}", "broadcaster", "{broadcaster:.*}")
	h.router.HandleFunc("/api/contents/{contentId:[0-9]+}/assets", h.assetsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/contents/{contentId:[0-9]+}/profiles/{profileId:[0-9]+}/assets", h.assetsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/assets", h.assetsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/assets/{id:[0-9]+}", h.assetsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/assets/{id:[0-9]+}/ffmpegLogs/current", h.ffmpegLogsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/assets/{id:[0-9]+}/ffmpegProgress/current", h.ffmpegProgressGetHandler).Methods("GET")
	h.router.HandleFunc("/api/assets/{assetId:[0-9]+}/assetsStreams", h.assetsStreamsPostHandler).Methods("POST")
	h.router.HandleFunc("/api/encoders", h.encodersGetHandler).Methods("GET")
	h.router.HandleFunc("/api/encoders/{id:[0-9]+}", h.encodersGetHandler).Methods("GET")
	h.router.HandleFunc("/api/logs", h.ffmpegLogsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/logs/{assetId:[0-9]+}", h.ffmpegLogsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/ffmpegProgress", h.ffmpegProgressGetHandler).Methods("GET").Queries("assetId", "{assetId:[0-9]+}")
	h.router.HandleFunc("/api/ffmpegProgress", h.ffmpegProgressGetHandler).Methods("GET")
	h.router.HandleFunc("/api/presets", h.presetsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/presets/{id:[0-9]+}", h.presetsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/profiles", h.profilesGetHandler).Methods("GET")
	h.router.HandleFunc("/api/profiles/{id:[0-9]+}", h.profilesGetHandler).Methods("GET")
	h.router.HandleFunc("/api/profiles/{profileId:[0-9]+}/contents", h.contentsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/profiles/{profileId:[0-9]+}/presets", h.presetsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/contentsStreams", h.contentsStreamsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/contentsStreams", h.contentsStreamsPostHandler).Methods("POST").Queries("contentId", "{contentId:[0-9]+}")
	h.router.HandleFunc("/api/contentsStreams/{contentsStreamId:[0-9]+}", h.contentsStreamsPutHandler).Methods("PUT")
	h.router.HandleFunc("/api/assetsStreams", h.assetsStreamsGetHandler).Methods("GET").Queries("md5Hash", "{md5Hash:[0-9a-f\\)]+}", "profileName", "{profileName:.*}", "broadcaster", "{broadcaster:.*}")
	h.router.HandleFunc("/api/assetsStreams", h.assetsStreamsGetHandler).Methods("GET").Queries("md5Hash", "{md5Hash:[0-9a-f\\)]+}")
	h.router.HandleFunc("/api/assetsStreams", h.assetsStreamsGetHandler).Methods("GET")
	h.router.HandleFunc("/api/assetsStreams", h.assetsStreamsPostHandler).Methods("POST")
	h.router.HandleFunc("/api/contentsMd5", h.contentsMd5PostHandler).Methods("POST")
	h.router.HandleFunc("/api/assetsStreams/{assetId:[0-9]+}", h.assetsStreamsPostHandler).Methods("POST")
	h.router.HandleFunc("/api/profilesParameters", h.profilesParametersGetHandler).Methods("GET")
	h.router.HandleFunc("/api/package", h.packagePostHandler).Methods("POST")
	h.router.HandleFunc("/api/transcode", h.transcodePostHandler).Methods("POST")
	//h.router.HandleFunc("/api/transcode/{uuid:[0-9a-f\\-]*}", h.transcodePostHandler).Methods("POST")
	//h.router.HandleFunc("/api/setSubtitles/{uuid:[0-9a-f\\-]*}", h.setSubtitlesPostHandler).Methods("POST")
	h.router.HandleFunc("/api/pfManifest", h.pfManifestGetHandler).Methods("GET").Queries("contentId", "{contentId:[0-9]+}", "broadcaster", "{broadcaster:[a-zA-Z]+}")
	h.router.HandleFunc("/api/pfAssetsChannels", h.pfAssetsChannelsGetHandler).Methods("GET").Queries("contentId", "{contentId:[0-9]+}", "broadcaster", "{broadcaster:[a-zA-Z]+}", "type", "{type:audio|video}")
	h.router.HandleFunc("/api/pfSubtitles", h.pfSubtitlesGetHandler).Methods("GET").Queries("contentId", "{contentId:[0-9]+}", "broadcaster", "{broadcaster:[a-zA-Z]+}")
	h.router.HandleFunc("/api/pfSubtitles", h.pfSubtitlesPostHandler).Methods("POST")
	h.router.HandleFunc("/api/pfContentsStreams", h.pfContentsStreamsPostHandler).Methods("POST")
	h.router.HandleFunc("/api/pfTranscode", h.pfTranscodePostHandler).Methods("POST")

	http.Handle("/", h.router)
	//
	h.initialized = true
	log.Printf("-- SchedulerHttpServerTask init done successfully")
	return h.initialized
}

func (h *SchedulerHttpServerTask) Start() {
	if h.initialized == false {
		log.Printf("SchedulerHttpServerTask not initialized, Thread cannot start...")
		return
	}
	log.Printf("-- SchedulerHttpServerTask Thread starting...")
	//
	//http.ListenAndServe(":4000", handlers.CORS()(r))
	go http.ListenAndServe(":4000", nil)
	//
	log.Printf("-- SchedulerHttpServerTask Thread started")
}

/* Handlers */

func (h *SchedulerHttpServerTask) optionsGetHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

// API Handlers
func (h *SchedulerHttpServerTask) contentsGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	params := mux.Vars(r)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	id := -1
	profileId := -1
	md5Hash := ""
	if params["id"] != "" {
		id, err = strconv.Atoi(params["id"])
		if err != nil {
			errStr := fmt.Sprintf("XX cannot convert id value '%s' to int: %s", id, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	if params["profileId"] != "" {
		profileId, err = strconv.Atoi(params["profileId"])
		if err != nil {
			errStr := fmt.Sprintf("XX cannot convert profileId value '%s' to int: %s", profileId, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	if params["md5Hash"] != "" {
		md5Hash = params["md5Hash"]
	}
	db := database.OpenDb()
	defer db.Close()

	var query string
	if md5Hash != "" {
		query = "SELECT * FROM contents WHERE md5Hash=?"
	} else {
		if id >= 0 {
			query = "SELECT * FROM contents WHERE contentId=?"
			if params["state"] != "" {
				query += " AND state=?"
			}
			if params["uuid"] != "" {
				query += " AND uuid=?"
			}
		} else {
			if profileId >= 0 {
				query = "SELECT c.* FROM contents AS c LEFT JOIN contentsProfiles AS cp ON c.contentId=cp.contentId WHERE cp.profileId=?"
				id = profileId
				if params["state"] != "" {
					query += " AND c.state=?"
				}
				if params["uuid"] != "" {
					query += " AND c.uuid=?"
				}
			} else {
				query = "SELECT * FROM contents"
				if params["state"] != "" && params["uuid"] != "" {
					query += " WHERE state=? AND uuid=?"
				} else {
					if params["state"] != "" {
						query += " WHERE state=?"
					} else {
						if params["uuid"] != "" {
							query += " WHERE uuid=?"
						}
					}
				}
			}
		}
	}

	log.Printf("Query is %s", query)
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	if md5Hash != "" {
		rows, err = stmt.Query(md5Hash)
	} else {
		if id == -1 {
			if params["uuid"] != "" && params["state"] != "" {
				rows, err = stmt.Query(params["state"], params["uuid"])
			} else {
				if params["state"] != "" {
					rows, err = stmt.Query(params["state"])
				} else {
					if params["uuid"] != "" {
						rows, err = stmt.Query(params["uuid"])
					} else {
						rows, err = stmt.Query()
					}
				}
			}
		} else {
			if params["uuid"] != "" && params["state"] != "" {
				rows, err = stmt.Query(id, params["state"], params["uuid"])
			} else {
				if params["state"] != "" {
					rows, err = stmt.Query(id, params["state"])
				} else {
					if params["uuid"] != "" {
						rows, err = stmt.Query(id, params["uuid"])
					} else {
						rows, err = stmt.Query(id)
					}
				}
			}
		}
	}

	defer rows.Close()
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	jsonAnswer := ""
	rowsNumber := 0
	for rows.Next() {
		var c database.Content
		err = rows.Scan(&c.ID, &c.Uuid, &c.Md5Hash, &c.Filename, &c.State, &c.Size, &c.Duration, &c.UspPackage, &c.Drm, &c.CreatedAt, &c.UpdatedAt)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows result for query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		query = "SELECT profileId FROM contentsProfiles WHERE contentId=?"
		stmt, err = db.Prepare(query)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		defer stmt.Close()
		var rows2 *sql.Rows
		rows2, err = stmt.Query(c.ID)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		defer rows2.Close()
		var profileId int
		for rows2.Next() {
			err = rows2.Scan(&profileId)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot scan rows result for query %s: %s", query, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			c.ProfileIds = append(c.ProfileIds, profileId)
		}
		b, err := json.Marshal(c)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", c, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	if rowsNumber > 0 {
		jsonAnswer = "[" + jsonAnswer[:len(jsonAnswer)-1] + "]"
	} else {
		jsonAnswer = "[ ]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) contentsPostHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	var jcc JsonCreateContent
	err := json.Unmarshal(body, &jcc)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot decode JSON %s: %s", body, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	var errMsg []string
	// Validate datas
	if jcc.Filename == nil {
		errMsg = append(errMsg, "'filename' is missing")
	}
	if errMsg != nil {
		w.Write([]byte(`{"error":"` + strings.Join(errMsg, ",") + `"}`))
		return
	}

	db := database.OpenDb()
	defer db.Close()

	// test if the file exists ang get informations
	var vfi VideoFileInfo
	vfi, err = getVideoFileInformations(*jcc.Filename)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot get informations from file %s: %s", *jcc.Filename, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	// Compute md5sum of file datas
	md5sum, err := exec.Command(`/usr/bin/md5sum`, *jcc.Filename).Output()
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute md5sum on %s: %s", *jcc.Filename, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	md5 := strings.Split(string(md5sum), ` `)[0]

	uuid := uuid.New()
	query := "INSERT INTO contents (`uuid`,`md5Hash`,`filename`,`size`,`duration`,`createdAt`) VALUES (?,?,?,?,?,NULL)"
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	result, err := stmt.Exec(uuid, md5, *jcc.Filename, vfi.Stat.Size(), vfi.Duration)
	if err != nil {
		errStr := fmt.Sprintf("XX Error during query execution %s with (%s,%s,%d,%d): %s", query, uuid, *jcc.Filename, vfi.Stat.Size(), vfi.Duration, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	var contentId int64
	contentId, err = result.LastInsertId()
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot get the last insert contentId with %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	for _, vs := range vfi.VideoStreams {
		stmt, err = db.Prepare("INSERT INTO contentsStreams (`contentId`,`mapId`,`type`,`language`,`codec`,`codecInfo`,`codecProfile`,`bitrate`,`width`,`height`,`fps`,`createdAt`) VALUES (?,?,'video',?,?,?,?,?,?,?,?,NULL)")
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		_, err = stmt.Exec(contentId, vs.Id, vs.Language, vs.Codec, vs.CodecInfo, vs.CodecProfile, vs.Bitrate, vs.Width, vs.Height, vs.Fps)
		if err != nil {
			errStr := fmt.Sprintf("XX Error during query execution %s with (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s): %s", query, contentId, vs.Id, vs.Language, vs.Codec, vs.CodecInfo, vs.CodecProfile, vs.Bitrate, vs.Width, vs.Height, vs.Fps, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			stmt.Close()
			return
		}
		stmt.Close()
	}
	for _, as := range vfi.AudioStreams {
		stmt, err = db.Prepare("INSERT INTO contentsStreams (`contentId`,`mapId`,`type`,`language`,`codec`,`codecInfo`,`bitrate`,`frequency`,`createdAt`) VALUES (?,?,'audio',?,?,?,?,?,NULL)")
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		_, err = stmt.Exec(contentId, as.Id, as.Language, as.Codec, as.CodecInfo, as.Bitrate, as.Frequency)
		if err != nil {
			errStr := fmt.Sprintf("XX Error during query execution %s with (%s,%s,%s,%s,%s,%s,%s): %s", query, contentId, as.Id, as.Language, as.Codec, as.CodecInfo, as.Bitrate, as.Frequency, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			stmt.Close()
			return
		}
		stmt.Close()
	}
	jsonAnswer := fmt.Sprintf(`{"contentId":%d,"uuid":"%s"}`, contentId, uuid)

	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) contentsStreamsGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	id := -1
	if params["id"] != "" {
		id, err = strconv.Atoi(params["id"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert id %s: %s", params["id"], err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}

	db := database.OpenDb()
	defer db.Close()

	var query string
	if id == -1 {
		query = "SELECT * FROM contentsStreams"
	} else {
		query = "SELECT * FROM contentsStreams WHERE contentId=?"
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var s database.ContentsStream
	var rows *sql.Rows
	if id == -1 {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(id)
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	rowsNumber := 0
	jsonAnswer := ""
	for rows.Next() {
		err = rows.Scan(&s.ID, &s.ContentId, &s.MapId, &s.Type, &s.Language, &s.Codec, &s.CodecInfo, &s.CodecProfile, &s.Bitrate, &s.Frequency, &s.Width, &s.Height, &s.Fps, &s.CreatedAt, &s.UpdatedAt)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		b, err := json.Marshal(s)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", s, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	if rowsNumber > 0 {
		jsonAnswer = "[" + jsonAnswer[:len(jsonAnswer)-1] + "]"
	} else {
		jsonAnswer = "[ ]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) assetsStreamsGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	id := -1
	contentId := -1
	md5Hash := ""
	profileName := ""
	broadcaster := ""
	if params["id"] != "" {
		id, err = strconv.Atoi(params["id"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert id %s: %s", params["id"], err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	if params["contentId"] != "" {
		contentId, err = strconv.Atoi(params["contentId"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert contentId %s: %s", params["contentId"], err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	if params["md5Hash"] != "" {
		md5Hash = params["md5Hash"]
	}
	if params["profileName"] != "" {
		profileName = params["profileName"]
	}
	if params["broadcaster"] != "" {
		broadcaster = params["broadcaster"]
	}

	db := database.OpenDb()
	defer db.Close()

	var query string
	if md5Hash != "" {
		query = "SELECT ass.* FROM assets AS a RIGHT JOIN assetsStreams AS ass ON a.assetId=ass.assetId WHERE contentId=(select contentId from contents where md5Hash=?)"
		if profileName != "" {
			query += " AND presetId IN (SELECT presetId FROM presets AS pr LEFT JOIN profiles AS p ON pr.profileId=p.profileId WHERE p.name=? AND p.broadcaster=?);"
		}
	} else {
		if id >= 0 {
			query = "SELECT * FROM assetsStreams WHERE assetId=?"
		} else {
			if contentId >= 0 {
				query = "SELECT ass.* FROM assetsStreams AS ass LEFT JOIN assets AS a ON a.assetId=ass.assetId WHERE a.contentId=?"
				id = contentId
			} else {
				query = "SELECT * FROM assetsStreams"
			}
		}
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var s database.AssetsStream
	var rows *sql.Rows
	if md5Hash != "" {
		if profileName != "" && broadcaster != "" {
			rows, err = stmt.Query(md5Hash, profileName, broadcaster)
		} else {
			rows, err = stmt.Query(md5Hash)
		}
	} else {
		if id >= 0 {
			rows, err = stmt.Query(id)
		} else {
			rows, err = stmt.Query()
		}
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	rowsNumber := 0
	jsonAnswer := ""
	for rows.Next() {
		err = rows.Scan(&s.AssetId, &s.MapId, &s.Type, &s.Language, &s.Codec, &s.CodecInfo, &s.CodecProfile, &s.Bitrate, &s.Frequency, &s.Width, &s.Height, &s.Fps, &s.CreatedAt, &s.UpdatedAt)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		b, err := json.Marshal(s)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", s, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	if rowsNumber > 0 {
		jsonAnswer = "[" + jsonAnswer[:len(jsonAnswer)-1] + "]"
	} else {
		jsonAnswer = "[ ]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) assetsGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	id := -1
	contentId := -1
	profileId := -1
	broadcaster := ""
	if params["id"] != "" {
		id, err = strconv.Atoi(params["id"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert id value '%s' to int: %s", id, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	if params["contentId"] != "" {
		contentId, err = strconv.Atoi(params["contentId"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert contentId value '%s' to int: %s", contentId, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	if params["profileId"] != "" {
		profileId, err = strconv.Atoi(params["profileId"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert profileId value '%s' to int: %s", profileId, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	var profileName = ""
	if params["profileName"] != "" {
		profileName = params["profileName"]
	}
	var presetsType = ""
	if params["presetsType"] != "" {
		presetsType = params["presetsType"]
	}
	if params["broadcaster"] != "" {
		broadcaster = params["broadcaster"]
	}
	db := database.OpenDb()
	defer db.Close()

	// select a.* from assets as a left join contentsProfiles as cp on a.contentId=cp.contentId where a.contentId='10' and cp.profileId=2;
	// select * from assets where contentId='719' AND presetId IN (SELECT presetId FROM presets AS pr LEFT JOIN profiles AS p ON pr.profileId=p.profileId WHERE p.name='VIDEO0ENG_AUDIO0FRA_BOUYGUES' AND pr.type='ffmpeg');

	var query string
	if profileName != "" {
		id = contentId
		query = "SELECT * FROM assets WHERE contentId=? AND presetId IN (SELECT presetId FROM presets AS pr LEFT JOIN profiles AS p ON pr.profileId=p.profileId WHERE p.name=? AND p.broadcaster=?"
		if presetsType != "" {
			query += " AND pr.type=?)"
		} else {
			query += ")"
		}
	} else {
		if id >= 0 {
			query = "SELECT * FROM assets WHERE assetId=?"
		} else {
			if contentId >= 0 {
				query = "SELECT * FROM assets WHERE contentId=?"
				id = contentId
				if profileId >= 0 {
					query = "SELECT a.* FROM assets AS a LEFT JOIN contentsProfiles AS cp ON a.contentId=cp.contentId WHERE a.contentId=? AND cp.profileId=?"
				}
			} else {
				query = "SELECT * FROM assets"
			}
		}
	}
	log.Printf("-- assetsGetHandler query is %s", query)
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var a database.Asset
	var rows *sql.Rows
	if profileName != "" && broadcaster != "" {
		if presetsType != "" {
			rows, err = stmt.Query(id, profileName, broadcaster, presetsType)
		} else {
			rows, err = stmt.Query(id, profileName)
		}
	} else {
		if id == -1 {
			rows, err = stmt.Query()
		} else {
			if profileId == -1 {
				rows, err = stmt.Query(id)
			} else {
				rows, err = stmt.Query(id, profileId)
			}
		}
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	rowsNumber := 0
	jsonAnswer := ""
	for rows.Next() {
		err = rows.Scan(&a.ID, &a.ContentId, &a.PresetId, &a.AssetIdDependance, &a.Filename, &a.DoAnalyze, &a.State, &a.CreatedAt, &a.UpdatedAt)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		b, err := json.Marshal(a)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", a, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	if rowsNumber > 0 {
		jsonAnswer = "[" + jsonAnswer[:len(jsonAnswer)-1] + "]"
	} else {
		jsonAnswer = "[ ]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) ffmpegLogsGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	id := -1
	if params["assetId"] != "" {
		id, err = strconv.Atoi(params["assetId"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert id value '%s' to int: %s", id, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	db := database.OpenDb()
	defer db.Close()

	var query string
	if id == -1 {
		query = "SELECT * FROM ffmpegLogs"
	} else {
		query = "SELECT * FROM ffmpegLogs WHERE assetId=?"
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var fl database.FfmpegLog
	var rows *sql.Rows
	if id == -1 {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(id)
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	rowsNumber := 0
	jsonAnswer := ""
	for rows.Next() {
		err = rows.Scan(&fl.AssetId, &fl.Log)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		b, err := json.Marshal(fl)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", fl, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	jsonAnswer = jsonAnswer[:len(jsonAnswer)-1]
	if rowsNumber > 1 {
		jsonAnswer = "[" + jsonAnswer + "]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) ffmpegProgressGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	assetId := -1
	if params["assetId"] != "" {
		assetId, err = strconv.Atoi(params["assetId"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert assetId value '%s' to int: %s", assetId, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	db := database.OpenDb()
	defer db.Close()

	var query string
	if assetId == -1 {
		query = "SELECT * FROM ffmpegProgress"
	} else {
		query = "SELECT * FROM ffmpegProgress WHERE assetId=?"
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var fp database.FfmpegProgress
	var rows *sql.Rows
	if assetId == -1 {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(assetId)
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	rowsNumber := 0
	jsonAnswer := ""
	for rows.Next() {
		err = rows.Scan(&fp.AssetId, &fp.Frame, &fp.Fps, &fp.Q, &fp.Size, &fp.Elapsed, &fp.Bitrate)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		b, err := json.Marshal(fp)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", fp, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	jsonAnswer = jsonAnswer[:len(jsonAnswer)-1]
	if rowsNumber > 1 {
		jsonAnswer = "[" + jsonAnswer + "]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) assetsStreamsPostHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	assetId := -1
	if params["assetId"] != "" {
		assetId, err = strconv.Atoi(params["assetId"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert id %s: %s", params["id"], err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}

	db := database.OpenDb()
	defer db.Close()

	var query string
	if assetId >= 0 {
		query = "SELECT assetId,filename,doAnalyze FROM assets WHERE assetId=?"
	} else {
		query = "SELECT assetId,filename,doAnalyze FROM assets"
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	if assetId >= 0 {
		rows, err = stmt.Query(assetId)
	} else {
		rows, err = stmt.Query()
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	jsonAnswer := ""
	var filename string
	var doAnalyze string
	for rows.Next() {
		err = rows.Scan(&assetId, &filename, &doAnalyze)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		var vfi VideoFileInfo
		vfi, err = getVideoFileInformations(filename)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot get informations from file %s: %s", filename, err)
			log.Printf(errStr)
			continue
		}
		if doAnalyze == `yes` {
			for _, vs := range vfi.VideoStreams {
				stmt, err = db.Prepare("INSERT INTO assetsStreams (`assetId`,`mapId`,`type`,`language`,`codec`,`codecInfo`,`codecProfile`,`bitrate`,`width`,`height`,`fps`,`createdAt`) VALUES (?,?,'video',?,?,?,?,?,?,?,?,NULL)")
				if err != nil {
					errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
					log.Printf(errStr)
					jsonStr := `{"error":"` + err.Error() + `"}`
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(jsonStr))
					return
				}
				_, err = stmt.Exec(assetId, vs.Id, vs.Language, vs.Codec, vs.CodecInfo, vs.CodecProfile, vs.Bitrate, vs.Width, vs.Height, vs.Fps)
				if err != nil {
					/*query = "UPDATE assetsStreams SET language=?,codec=?,codecInfo=?,codecProfile=?,bitrate=?,width=?,height=?,fps=?,updatedAt=NOW() WHERE assetId=? AND mapId=? AND type='video'"
					  var stmt2 *sql.Stmt
					  stmt2, err = db.Prepare(query)
					  if err != nil {
					    errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
					    log.Printf(errStr)
					    jsonStr := `{"error":"` + err.Error() + `"}`
					    w.WriteHeader(http.StatusNotFound)
					    w.Write([]byte(jsonStr))
					    stmt.Close()
					    return
					  }
					  _, err = stmt2.Exec(vs.Language, vs.Codec, vs.CodecInfo, vs.CodecProfile, vs.Bitrate, vs.Width, vs.Height, vs.Fps, assetId, vs.Id)
					  if err != nil {
					    errStr := fmt.Sprintf("XX Error during query execution %s with (%s,%s,%s,%s,%s,%s,%s,%s) WHERE (%d,%s): %s", query, vs.Language, vs.Codec, vs.CodecInfo, vs.CodecProfile, vs.Bitrate, vs.Width, vs.Height, vs.Fps, assetId, vs.Id, err)
					    log.Printf(errStr)
					    jsonStr := `{"error":"` + err.Error() + `"}`
					    w.WriteHeader(http.StatusNotFound)
					    w.Write([]byte(jsonStr))
					    stmt2.Close()
					    stmt.Close()
					    return
					  }
					  stmt2.Close()*/
				}
				stmt.Close()
			}
			for _, as := range vfi.AudioStreams {
				stmt, err = db.Prepare("INSERT INTO assetsStreams (`assetId`,`mapId`,`type`,`language`,`codec`,`codecInfo`,`bitrate`,`frequency`,`createdAt`) VALUES (?,?,'audio',?,?,?,?,?,NULL)")
				if err != nil {
					errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
					log.Printf(errStr)
					jsonStr := `{"error":"` + err.Error() + `"}`
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(jsonStr))
					return
				}
				_, err = stmt.Exec(assetId, as.Id, as.Language, as.Codec, as.CodecInfo, as.Bitrate, as.Frequency)
				if err != nil {
					/*query = "UPDATE assetsStreams SET language=?,codec=?,codecInfo=?,bitrate=?,frequency=?,updatedAt=NOW() WHERE assetId=? AND mapId=? AND type='audio'"
					  var stmt2 *sql.Stmt
					  stmt2, err = db.Prepare(query)
					  if err != nil {
					    errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
					    log.Printf(errStr)
					    jsonStr := `{"error":"` + err.Error() + `"}`
					    w.WriteHeader(http.StatusNotFound)
					    w.Write([]byte(jsonStr))
					    stmt.Close()
					    return
					  }
					  _, err = stmt2.Exec(as.Language, as.Codec, as.CodecInfo, as.Bitrate, as.Frequency, assetId, as.Id)
					  if err != nil {
					    errStr := fmt.Sprintf("XX Error during query execution %s with (%s,%s,%s,%s,%s) WHERE (%d,%s): %s", query, as.Language, as.Codec, as.CodecInfo, as.Bitrate, as.Frequency, assetId, as.Id, err)
					    log.Printf(errStr)
					    jsonStr := `{"error":"` + err.Error() + `"}`
					    w.WriteHeader(http.StatusNotFound)
					    w.Write([]byte(jsonStr))
					    stmt2.Close()
					    stmt.Close()
					    return
					  }
					  stmt2.Close()*/
				}
				stmt.Close()
			}
		}
	}
	jsonAnswer = `{"result":"success"}`
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) encodersGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	id := -1
	if params["id"] != "" {
		id, err = strconv.Atoi(params["id"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert id value '%s' to int: %s", id, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	db := database.OpenDb()
	defer db.Close()

	var query string
	if id == -1 {
		query = "SELECT * FROM encoders"
	} else {
		query = "SELECT * FROM encoders WHERE encoderId=?"
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var e database.Encoder
	var rows *sql.Rows
	if id == -1 {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(id)
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	rowsNumber := 0
	jsonAnswer := ""
	for rows.Next() {
		err = rows.Scan(&e.ID, &e.Hostname, &e.ActiveTasks, &e.Load1, &e.UpdatedAt)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		b, err := json.Marshal(e)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", e, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	jsonAnswer = jsonAnswer[:len(jsonAnswer)-1]
	if rowsNumber > 1 {
		jsonAnswer = "[" + jsonAnswer + "]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) presetsGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	id := -1
	profileId := -1
	if params["id"] != "" {
		id, err = strconv.Atoi(params["id"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert id value '%s' to int: %s", id, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	if params["profileId"] != "" {
		profileId, err = strconv.Atoi(params["profileId"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert profileId value '%s' to int: %s", profileId, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	db := database.OpenDb()
	defer db.Close()

	var query string
	if id >= 0 {
		query = "SELECT * FROM presets WHERE presetId=?"
	} else {
		if profileId >= 0 {
			query = "SELECT * FROM presets WHERE profileId=?"
			id = profileId
		} else {
			query = "SELECT * FROM presets"
		}
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var p database.Preset
	var rows *sql.Rows
	if id == -1 {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(id)
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	rowsNumber := 0
	jsonAnswer := ""
	for rows.Next() {
		err = rows.Scan(&p.ID, &p.ProfileId, &p.PresetIdDependance, &p.Name, &p.Type, &p.DoAnalyze, &p.CmdLine, &p.CreatedAt, &p.UpdatedAt)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		b, err := json.Marshal(p)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", p, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	jsonAnswer = jsonAnswer[:len(jsonAnswer)-1]
	if rowsNumber > 1 {
		jsonAnswer = "[" + jsonAnswer + "]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) profilesGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	id := -1
	if params["id"] != "" {
		id, err = strconv.Atoi(params["id"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert id value '%s' to int: %s", id, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	db := database.OpenDb()
	defer db.Close()

	var query string
	if id == -1 {
		query = "SELECT `profileId`,`name`,`broadcaster`,`acceptSubtitles`,`createdAt`,`updatedAt` FROM profiles"
	} else {
		query = "SELECT `profileId`,`name`,`broadcaster`,`acceptSubtitles`,`createdAt`,`updatedAt` FROM profiles WHERE profileId=?"
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var p database.Profile
	var rows *sql.Rows
	if id == -1 {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(id)
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	rowsNumber := 0
	jsonAnswer := ""
	for rows.Next() {
		err = rows.Scan(&p.ID, &p.Name, &p.Broadcaster, &p.AcceptSubtitles, &p.CreatedAt, &p.UpdatedAt)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		b, err := json.Marshal(p)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", p, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	jsonAnswer = jsonAnswer[:len(jsonAnswer)-1]
	if rowsNumber > 1 {
		jsonAnswer = "[" + jsonAnswer + "]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) contentsStreamsPostHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	params := mux.Vars(r)
	contentId := -1
	if params["contentId"] != "" {
		contentId, err = strconv.Atoi(params["contentId"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert contentId %s: %s", params["contentId"], err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}

	db := database.OpenDb()
	defer db.Close()

	var query string
	query = "SELECT contentId,filename FROM contents WHERE contentId=?"
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(contentId)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s (%d): %s", query, contentId, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	jsonAnswer := ""
	var filename string
	for rows.Next() {
		err = rows.Scan(&contentId, &filename)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		var vfi VideoFileInfo
		vfi, err = getVideoFileInformations(filename)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot get informations from file %s: %s", filename, err)
			log.Printf(errStr)
			continue
		}
		for _, vs := range vfi.VideoStreams {
			stmt, err = db.Prepare("INSERT INTO contentsStreams (`contentId`,`mapId`,`type`,`language`,`codec`,`codecInfo`,`codecProfile`,`bitrate`,`width`,`height`,`fps`,`createdAt`) VALUES (?,?,'video',?,?,?,?,?,?,?,?,NULL)")
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			defer stmt.Close()
			_, err = stmt.Exec(contentId, vs.Id, vs.Language, vs.Codec, vs.CodecInfo, vs.CodecProfile, vs.Bitrate, vs.Width, vs.Height, vs.Fps)
			if err != nil {
				query = "UPDATE contentsStreams SET language=?,codec=?,codecInfo=?,codecProfile=?,bitrate=?,width=?,height=?,fps=?,updatedAt=NOW() WHERE contentId=? AND mapId=? AND type='video'"
				stmt, err = db.Prepare(query)
				if err != nil {
					errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
					log.Printf(errStr)
					jsonStr := `{"error":"` + err.Error() + `"}`
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(jsonStr))
					return
				}
				defer stmt.Close()
				_, err = stmt.Exec(vs.Language, vs.Codec, vs.CodecInfo, vs.CodecProfile, vs.Bitrate, vs.Width, vs.Height, vs.Fps, contentId, vs.Id)
				if err != nil {
					errStr := fmt.Sprintf("XX Error during query execution %s with (%s,%s,%s,%s,%s,%s,%s,%s) WHERE (%d,%s): %s", query, vs.Language, vs.Codec, vs.CodecInfo, vs.CodecProfile, vs.Bitrate, vs.Width, vs.Height, vs.Fps, contentId, vs.Id, err)
					log.Printf(errStr)
					jsonStr := `{"error":"` + err.Error() + `"}`
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(jsonStr))
					return
				}
			}
		}
		for _, as := range vfi.AudioStreams {
			stmt, err = db.Prepare("INSERT INTO contentsStreams (`contentId`,`mapId`,`type`,`language`,`codec`,`codecInfo`,`bitrate`,`frequency`,`createdAt`) VALUES (?,?,'audio',?,?,?,?,?,NULL)")
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			defer stmt.Close()
			_, err = stmt.Exec(contentId, as.Id, as.Language, as.Codec, as.CodecInfo, as.Bitrate, as.Frequency)
			if err != nil {
				query = "UPDATE contentsStreams SET language=?,codec=?,codecInfo=?,bitrate=?,frequency=?,updatedAt=NOW() WHERE contentId=? AND mapId=? AND type='audio'"
				stmt, err = db.Prepare(query)
				if err != nil {
					errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
					log.Printf(errStr)
					jsonStr := `{"error":"` + err.Error() + `"}`
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(jsonStr))
					return
				}
				defer stmt.Close()
				_, err = stmt.Exec(as.Language, as.Codec, as.CodecInfo, as.Bitrate, as.Frequency, contentId, as.Id)
				if err != nil {
					errStr := fmt.Sprintf("XX Error during query execution %s with (%s,%s,%s,%s,%s,%s,%s) WHERE (%d,%s): %s", query, as.Language, as.Codec, as.CodecInfo, as.Bitrate, as.Frequency, contentId, as.Id, err)
					log.Printf(errStr)
					jsonStr := `{"error":"` + err.Error() + `"}`
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(jsonStr))
					return
				}
			}
		}
	}
	jsonAnswer = `{"result":"success"}`
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) contentsStreamsPutHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	body, _ := ioutil.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	var cspj ContentsStreamsPutJson
	err = json.Unmarshal(body, &cspj)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot decode JSON %s: %s", body, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	if cspj.Language == nil {
		errStr := fmt.Sprintf("XX Language JSON parameter is missing")
		log.Printf(errStr)
		jsonStr := `{"error":"'language' parameter is missing"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	params := mux.Vars(r)

	db := database.OpenDb()
	defer db.Close()

	var query string
	query = "UPDATE contentsStreams SET language=? WHERE contentsStreamId=?"
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	_, err = stmt.Exec(cspj.Language, params["contentsStreamId"])
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	jsonStr := `{"result":"success"}`
	w.Write([]byte(jsonStr))
}

func (h *SchedulerHttpServerTask) contentsMd5PostHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	db := database.OpenDb()
	defer db.Close()

	query := "SELECT contentId, filename FROM contents WHERE md5Hash IS NULL"
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query()
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	var filename string
	var contentId int
	for rows.Next() {
		err = rows.Scan(&contentId, &filename)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan row for query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		md5sum, err := exec.Command(`/usr/bin/md5sum`, filename).Output()
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot execute md5sum for %s: %s", filename, err)
			log.Printf(errStr)
			continue
		}
		md5 := strings.Split(string(md5sum), ` `)[0]
		query = "UPDATE contents SET md5Hash=? WHERE contentId=?"
		var stmt2 *sql.Stmt
		stmt2, err = db.Prepare(query)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		_, err = stmt2.Exec(md5, contentId)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot execute query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			stmt2.Close()
			return
		}
	}

	jsonStr := `{"result":"success"}`
	w.Write([]byte(jsonStr))

	return
}

func (h *SchedulerHttpServerTask) profilesParametersGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	id := -1
	if params["presetParameterId"] != "" {
		id, err = strconv.Atoi(params["presetParameterId"])
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot convert id value '%s' to int: %s", id, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}

	db := database.OpenDb()
	defer db.Close()

	var query string
	if id == -1 {
		query = "SELECT * FROM profilesParameters"
	} else {
		query = "SELECT * FROM profilesParameters WHERE profileParameterId=?"
	}
	stmt, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var pp database.ProfilesParameter
	var rows *sql.Rows
	if id == -1 {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(id)
	}
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query rows for %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	rowsNumber := 0
	jsonAnswer := ""
	for rows.Next() {
		err = rows.Scan(&pp.ID, &pp.ProfileId, &pp.Parameter, &pp.Value, &pp.CreatedAt, &pp.UpdatedAt)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows of query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		b, err := json.Marshal(pp)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot JSON Marshal %#v: %s", pp, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonAnswer += string(b) + ","
		rowsNumber++
	}
	if rowsNumber > 0 {
		jsonAnswer = jsonAnswer[:len(jsonAnswer)-1]
	}
	if rowsNumber > 1 {
		jsonAnswer = "[" + jsonAnswer + "]"
	}
	w.Write([]byte(jsonAnswer))
}

func (h *SchedulerHttpServerTask) packagePostHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	body, _ := ioutil.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	var jpc JsonPackageContent
	err = json.Unmarshal(body, &jpc)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot decode JSON %s: %s", body, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	var errMsg []string
	// Validate datas
	if jpc.ContentId == nil {
		errMsg = append(errMsg, "'contentId' is missing")
	}
	if errMsg != nil {
		w.Write([]byte(`{"error":"` + strings.Join(errMsg, ",") + `"}`))
		return
	}
	db := database.OpenDb()
	defer db.Close()

	var cuuids []ContentsUuid
	for _, cId := range *jpc.ContentId {
		var cuuid ContentsUuid
		query := "SELECT uuid FROM contents WHERE contentId=?"
		stmt, err := db.Prepare(query)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		defer stmt.Close()
		err = stmt.QueryRow(cId).Scan(&cuuid.Uuid)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot query row for query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		cuuid.ContentId = cId
		cuuids = append(cuuids, cuuid)
	}

	err = packageContents(cuuids)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot pacakge contents %#v: %s", cuuids, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	jsonStr := `{"success":true}`
	w.Write([]byte(jsonStr))
}

func (h *SchedulerHttpServerTask) transcodePostHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	m := map[string]interface{}{}
	err := json.Unmarshal(body, &m)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot decode JSON %s: %s", body, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	var errMsg []string
	// Validate datas
	if m["profileId"] == nil {
		errMsg = append(errMsg, "'profileId' is missing")
	}
	if errMsg != nil {
		w.Write([]byte(`{"error":"` + strings.Join(errMsg, ",") + `"}`))
		return
	}

	var contentId int
	if m["uuid"] != nil && m["uuid"].(string) != "" {
		contentId, err = uuidToContentId(m["uuid"].(string))
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot get contenId from uuid %s: %s", params["uuid"], err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	if m["md5Hash"] != nil && m["md5Hash"].(string) != "" {
		contentId, err = md5HashToContentId(m["md5Hash"].(string))
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot get contenId from md5Hash %s: %s", m["md5Hash"].(string), err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}

	err = transcode(w, r, m, contentId)

	if err == nil {
	}
}

func (h *SchedulerHttpServerTask) pfManifestGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	if params["broadcaster"] == "" {
		errStr := fmt.Sprintf("XX broadcaster JSON parameter is missing")
		log.Printf(errStr)
		jsonStr := `{"error":"'broadcaster' parameter is missing"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	if params["contentId"] == "" {
		errStr := fmt.Sprintf("XX contentId JSON parameter is missing")
		log.Printf(errStr)
		jsonStr := `{"error":"'contentId' parameter is missing"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	db := database.OpenDb()
	defer db.Close()

	query := "SELECT state, cmdLine FROM assets AS a LEFT JOIN presets AS p ON a.presetId=p.presetId LEFT JOIN profiles AS pr ON p.profileId=pr.profileId WHERE a.contentId=? AND p.cmdLine LIKE '%usp_package%' AND pr.broadcaster=?"
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var state string
	var cmdLine string
	err = stmt.QueryRow(params["contentId"], strings.ToUpper(params["broadcaster"])).Scan(&state, &cmdLine)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot Scan result query %s with (%s, %s): %s", query, params["contentId"], strings.ToUpper(params["broadcaster"]), err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	if state == `ready` {
		query := "SELECT uuid, filename FROM contents WHERE contentId=?"
		var stmt *sql.Stmt
		stmt, err = db.Prepare(query)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		defer stmt.Close()
		var uuid string
		var filenamePath string
		err = stmt.QueryRow(params["contentId"]).Scan(&uuid, &filenamePath)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot Scan result query %s with (%s): %s", query, params["contentId"], err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		filename := filenamePath[len(path.Dir(filenamePath))+1:]
		vodDir := filename[:len(filename)-len(path.Ext(filename))]
		re := regexp.MustCompile(`[^ ]+\.ism`)
		matches := re.FindStringSubmatch(cmdLine)
		if matches != nil {
			log.Printf("matches is %#v", matches)
			ism := strings.Replace(matches[0], `%UUID%`, uuid, 1)
			manifestBaseUrl := `/vod/` + vodDir
			jsonStr := `{"manifests":[{"type":"dash","url":"` + manifestBaseUrl + `/` + ism + `/` + uuid + `.mpd"},{"type":"hls","url":"` + manifestBaseUrl + `/` + ism + `/` + uuid + `.m3u8"},{"type":"smooth","url":"` + manifestBaseUrl + `/` + ism + `/Manifest"}]}`
			w.Write([]byte(jsonStr))
			return
		} else {
			errStr := fmt.Sprintf("XX Cannot found ism package from cmdLine")
			log.Printf(errStr)
			jsonStr := `{"error":"cannot found ism package from db"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	jsonStr := `{"error":"manifest not found or not ready"}`
	w.Write([]byte(jsonStr))

	return
}

func (h *SchedulerHttpServerTask) pfAssetsChannelsGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	if params["broadcaster"] == "" {
		errStr := fmt.Sprintf("XX broadcaster JSON parameter is missing")
		log.Printf(errStr)
		jsonStr := `{"error":"'broadcaster' parameter is missing"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	if params["contentId"] == "" {
		errStr := fmt.Sprintf("XX contentId JSON parameter is missing")
		log.Printf(errStr)
		jsonStr := `{"error":"'contentId' parameter is missing"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	if params["type"] == "" {
		errStr := fmt.Sprintf("XX type JSON parameter is missing")
		log.Printf(errStr)
		jsonStr := `{"error":"'type' parameter is missing"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	db := database.OpenDb()
	defer db.Close()

	query := "SELECT ass.mapId,ass.language,ass.codec,ass.codecInfo,ass.codecProfile,ass.bitrate,ass.frequency,ass.width,ass.height,ass.fps FROM assets AS a LEFT JOIN assetsStreams AS ass ON a.assetId=ass.assetId WHERE a.contentId=? AND ass.type=? AND a.presetId IN (SELECT presetId FROM presets AS pr LEFT JOIN profiles AS p ON p.profileId=pr.profileId WHERE p.broadcaster=?)"
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(params["contentId"], params["type"], strings.ToUpper(params["broadcaster"]))
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute query %s with (%s,%s): %s", query, params["contentId"], strings.ToUpper(params["broadcaster"]), err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	jsonStr := `{"channels":[`
	rowsEmpty := true
	for rows.Next() {
		rowsEmpty = false
		var mapId int
		var language string
		var codec string
		var codecInfo string
		var codecProfile *string
		var bitrate int
		var frequency *int
		var width *int
		var height *int
		var fps *int
		err = rows.Scan(&mapId, &language, &codec, &codecInfo, &codecProfile, &bitrate, &frequency, &width, &height, &fps)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot get row for query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		if params["type"] == "audio" {
			jsonStr += fmt.Sprintf(`{"mapId":%d,"language":"%s","codec":"%s","codecInfo":"%s","bitrate":%d,"frequency":%d},`, mapId, language, codec, codecInfo, bitrate, *frequency)
		} else {
			jsonStr += fmt.Sprintf(`{"mapId":%d,"language":"%s","codec":"%s","codecInfo":"%s","codecProfile":"%s","bitrate":%d,"width":%d,"height":%d,"fps":%d},`, mapId, language, codec, codecInfo, *codecProfile, bitrate, *width, *height, *fps)
		}
	}
	if rowsEmpty == false {
		jsonStr = jsonStr[:len(jsonStr)-1]
	}
	jsonStr += `]}`
	w.Write([]byte(jsonStr))

	return
}

func (h *SchedulerHttpServerTask) pfSubtitlesGetHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	if params["contentId"] == "" {
		errStr := fmt.Sprintf("XX contentId JSON parameter is missing")
		log.Printf(errStr)
		jsonStr := `{"error":"'contentId' parameter is missing"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	if params["broadcaster"] == "" {
		errStr := fmt.Sprintf("XX broadcaster JSON parameter is missing")
		log.Printf(errStr)
		jsonStr := `{"error":"'broadcaster' parameter is missing"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	db := database.OpenDb()
	defer db.Close()

	query := "SELECT name FROM profiles AS p LEFT JOIN contentsProfiles AS cp ON p.profileId=cp.profileId WHERE contentId=? AND name LIKE ?"
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var profileName string
	err = stmt.QueryRow(params["contentId"], `%SUB%_`+strings.ToUpper(params["broadcaster"])).Scan(&profileName)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute query %s with (%s): %s", query, params["contentId"])
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	// Filter with Regexp to remove all subtitles not corresponding to SUB[0-9]([A-Z]{3}) eg SUB0FRA -> lang='fra', SUB0FRA_SUB0ENG -> lang IN ('fra', 'eng')
	re := regexp.MustCompile(`SUB[0-9]([A-Z]{3})`)
	matches := re.FindStringSubmatch(profileName)

	if matches != nil {
		langs := ``
		log.Printf("matches is %#v", matches)
		for _, m := range matches[1:] {
			langs += `'` + strings.ToLower(m) + `',`
		}
		langs = langs[:len(langs)-1]
		query = `SELECT url,lang FROM subtitles WHERE contentId=? AND lang IN (` + langs + `)`
	} else {
		query = "SELECT url,lang FROM subtitles WHERE contentId=?"
	}
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(params["contentId"])
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute query %s with (%s): %s", query, params["contentId"], err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	jsonStr := `{"subtitles":[`
	rowsEmpty := true
	for rows.Next() {
		var lang string
		var url string
		err = rows.Scan(&lang, &url)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot get row for query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		jsonStr += fmt.Sprintf(`{"lang":"%s","url":"%s"},`, lang, url)
		rowsEmpty = false
	}
	if rowsEmpty == false {
		jsonStr = jsonStr[:len(jsonStr)-1]
	}
	jsonStr += `]}`
	w.Write([]byte(jsonStr))

	return
}

func (h *SchedulerHttpServerTask) pfSubtitlesPostHandler(w http.ResponseWriter, r *http.Request) {
	var filesToMove []string

	body, _ := ioutil.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	var jss JsonSetSubtitles
	err := json.Unmarshal(body, &jss)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot decode JSON %s: %s", body, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	var errMsg []string
	// Validate datas
	if jss.ContentId == nil {
		errMsg = append(errMsg, "'contentId' is missing")
	}
	if jss.Subtitles == nil {
		errMsg = append(errMsg, "'subtitles' is missing")
	} else {
		for i, s := range *jss.Subtitles {
			if s.Lang == nil {
				errMsg = append(errMsg, fmt.Sprintf(`'lang' is missing on 'subtitles' index %d`, i))
			}
			if s.Url == nil {
				errMsg = append(errMsg, fmt.Sprintf(`'url' is missing on 'subtitles' index %d`, i))
			}
		}
	}
	if errMsg != nil {
		w.Write([]byte(`{"error":"` + strings.Join(errMsg, ",") + `"}`))
		return
	}

	db := database.OpenDb()
	defer db.Close()

	query2 := `DELETE FROM subtitles WHERE contentId=?`
	var stmt2 *sql.Stmt
	stmt2, err = db.Prepare(query2)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query2, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt2.Close()

	query := "INSERT INTO subtitles (`contentId`,`lang`,`url`) VALUES (?,?,?)"
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()

	query3 := `SELECT p.name, p.profileId, p.broadcaster, acceptSubtitles FROM contentsProfiles AS cp LEFT JOIN profiles AS p ON cp.profileId=p.profileId WHERE contentId=?`
	var stmt3 *sql.Stmt
	stmt3, err = db.Prepare(query3)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query3, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt3.Close()

	query4 := `SELECT name, profileId FROM profiles WHERE broadcaster=? AND acceptSubtitles='yes'`
	var stmt4 *sql.Stmt
	stmt4, err = db.Prepare(query4)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query4, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	_, err = stmt2.Exec(*jss.ContentId)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute query %s with (%d): %s", query2, *jss.ContentId, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	for _, s := range *jss.Subtitles {
		_, err = stmt.Exec(*jss.ContentId, *s.Lang, *s.Url)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot execute query %s with (%d,%s,%s): %s", query, *jss.ContentId, *s.Lang, *s.Url, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		err = getSubtitles(*s.Url, `/space/videos/encoded/tmp/`+path.Base(*s.Url))
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot GET url %s: %s", query, *s.Url, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		filesToMove = append(filesToMove, `/space/videos/encoded/tmp/`+path.Base(*s.Url))
	}

	var rows *sql.Rows
	rows, err = stmt3.Query(*jss.ContentId)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute query %s with (%d): %s", query3, *jss.ContentId, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()

	var profileToRepackage []int
	var profileToReplace []ChangeProfile
	for rows.Next() {
		var profileName string
		var profileId int
		var acceptSubs string
		var broadcaster string
		err = rows.Scan(&profileName, &profileId, &broadcaster, &acceptSubs)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows for query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		if acceptSubs == `yes` && (strings.Contains(profileName, `SUB`) == false) {
			profileToRepackage = append(profileToRepackage, profileId)
			continue
		}
		var rows2 *sql.Rows
		rows2, err = stmt4.Query(broadcaster)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot execute query %s with (%s): %s", query4, broadcaster, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		reProfile := regexp.MustCompile(`^(VIDEO[0-9]+[A-Z]{3}_(AUDIO[0-9]+[A-Z]{3}_)+).*$`)
		subLangAvailable := make(map[string]int)
		for rows2.Next() {
			var profileName2 string
			var profileId2 int
			err = rows2.Scan(&profileName2, &profileId2)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot scan rows for query %s: %s", query, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			matches := reProfile.FindStringSubmatch(profileName)
			if matches == nil {
				continue
			}
			log.Printf("profileName is %s, acceptSubs is %d, broadcaster is %s matches is %s", profileName, acceptSubs, broadcaster, matches[1])
			re := regexp.MustCompile(matches[1] + `SUB[0-9]([A-Z]{3})`)
			matches2 := re.FindStringSubmatch(profileName2)
			if matches2 != nil {
				subLangAvailable[strings.ToLower(matches2[1])] = profileId2
			}
		}
		for _, s := range *jss.Subtitles {
			if subLangAvailable[*s.Lang] != 0 {
				log.Printf("found subtitle lang %s with change profile %#v for broadcaster %s", *s.Lang, subLangAvailable[*s.Lang], broadcaster)
				var cp ChangeProfile
				cp.oldProfileId = profileId
				cp.newProfileId = subLangAvailable[*s.Lang]
				profileToReplace = append(profileToReplace, cp)
			}
		}
	}

	// Get destination path from table contents
	query = "SELECT filename FROM contents WHERE contentId=?"
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query4, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	var filename string
	err = stmt.QueryRow(*jss.ContentId).Scan(&filename)
	destBasePath := filename[:len(filename)-len(path.Ext(filename))]

	for _, f := range filesToMove {
		newDir := `/space/videos/encoded/origin/vod/` + path.Base(destBasePath)
		_ = os.Mkdir(newDir, 0755)
		newPath := newDir + `/` + path.Base(f)
		err = os.Rename(f, strings.Replace(newPath, " ", "_", -1))
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot move file from %s to %s: %s", f, newPath, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}

	// Depending on profile, set state='scheduled' for packaging task or delete old profile and add new one for subtitles burned on the video
	log.Printf("profile to repackage is %#v", profileToRepackage)
	log.Printf("profile to replace is %#v", profileToReplace)
	for _, p := range profileToRepackage {
		query = `UPDATE assets AS a LEFT JOIN presets AS p ON a.presetId=p.presetId SET state='scheduled' WHERE contentId=? AND p.profileId=? AND type='script' AND cmdLine LIKE '%package%'`
		stmt, err = db.Prepare(query)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		_, err = stmt.Exec(*jss.ContentId, p)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot execute query %s with (%d,%d): %s", query, *jss.ContentId, p, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}
	for _, p := range profileToReplace {
		if p.oldProfileId == p.newProfileId {
			query = `UPDATE assets AS a LEFT JOIN presets AS p ON a.presetId=p.presetId SET state='scheduled' WHERE contentId=? AND p.profileId=?`
			stmt, err = db.Prepare(query)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			_, err = stmt.Exec(*jss.ContentId, p.newProfileId)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot execute query %s with (%d,%d): %s", query, *jss.ContentId, p.newProfileId, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
		} else {
			query = "DELETE FROM contensProfiles WHERE contentId=? AND profileId=?"
			stmt, err = db.Prepare(query)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			_, err = stmt.Exec(*jss.ContentId, p.newProfileId)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot execute query %s with (%d,%d): %s", query, *jss.ContentId, p.newProfileId, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			query = "DELETE FROM assets AS a LEFT JOIN presets AS p ON a.presetId=p.presetId WHERE contentId=? AND p.profileId=?"
			stmt, err = db.Prepare(query)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			_, err = stmt.Exec(*jss.ContentId, p.oldProfileId)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot execute query %s with (%d,%d): %s", query, *jss.ContentId, p.oldProfileId, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			m := make(map[string]interface{})
			m["profileId"] = p.newProfileId
			transcode(w, r, m, *jss.ContentId)
		}
	}

	jsonStr := `{"result":"success"}`
	w.Write([]byte(jsonStr))

	return
}

func (h *SchedulerHttpServerTask) pfContentsStreamsPostHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	var jscs JsonSetContentsStreams
	err := json.Unmarshal(body, &jscs)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot decode JSON %s: %s", body, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	var errMsg []string
	// Validate datas
	if jscs.ContentId == nil {
		errMsg = append(errMsg, "'contentId' is missing")
	}
	if jscs.Streams == nil {
		errMsg = append(errMsg, "'streams' is missing")
	} else {
		for i, s := range *jscs.Streams {
			if s.Type == nil {
				errMsg = append(errMsg, fmt.Sprintf(`'type' is missing on 'subtitles' index %d`, i))
			}
			if s.Channel == nil {
				errMsg = append(errMsg, fmt.Sprintf(`'channel' is missing on 'subtitles' index %d`, i))
			}
			if s.Lang == nil {
				errMsg = append(errMsg, fmt.Sprintf(`'lang' is missing on 'subtitles' index %d`, i))
			}
		}
	}
	if errMsg != nil {
		w.Write([]byte(`{"error":"` + strings.Join(errMsg, ",") + `"}`))
		return
	}

	db := database.OpenDb()
	defer db.Close()

	for _, s := range *jscs.Streams {
		query := `UPDATE contentsStreams SET language=? WHERE contentId=? AND mapId=? AND type=?`
		var stmt *sql.Stmt
		stmt, err = db.Prepare(query)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		defer stmt.Close()
		_, err = stmt.Exec(*s.Lang, *jscs.ContentId, *s.Channel, *s.Type)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot execute query %s with (%d,%s,%s): %s", query, *s.Channel, *s.Type, *s.Lang, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}

	jsonStr := `{"result":"success"}`
	w.Write([]byte(jsonStr))

	return
}

func (h *SchedulerHttpServerTask) pfTranscodePostHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	var jt JsonTranscode
	err := json.Unmarshal(body, &jt)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot decode JSON %s: %s", body, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	var errMsg []string
	// Validate datas
	if jt.ContentId == nil {
		errMsg = append(errMsg, "'contentId' is missing")
	}
	if jt.Broadcaster == nil {
		errMsg = append(errMsg, "'broadcaster' is missing")
	}
	if errMsg != nil {
		w.Write([]byte(`{"error":"` + strings.Join(errMsg, ",") + `"}`))
		return
	}

	db := database.OpenDb()
	defer db.Close()

	query := `SELECT type,language FROM contentsStreams WHERE contentId=? AND (type='audio' OR type='video') ORDER BY type,mapId`
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(*jt.ContentId)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute query %s with (%d): %s", query, *jt.ContentId, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	likeProfile := ``
	likeProfileSubBurned := ``
	videoChannel := 0
	audioChannel := 0
	for rows.Next() {
		var streamType string
		var lang string
		err = rows.Scan(&streamType, &lang)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows for query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		if streamType == "video" && videoChannel == 0 {
			likeProfile += fmt.Sprintf(`%s%d%s_`, strings.ToUpper(streamType), videoChannel, strings.ToUpper(lang))
			videoChannel++
		} else {
			if streamType == "audio" && audioChannel == 0 {
				likeProfile += fmt.Sprintf(`%s%d%s`, strings.ToUpper(streamType), audioChannel, strings.ToUpper(lang))
				audioChannel++
			} else {
				if streamType == "video" {
					likeProfile += fmt.Sprintf(`(_%s%d%s)?`, strings.ToUpper(streamType), videoChannel, strings.ToUpper(lang))
				} else {
					if streamType == "audio" {
						likeProfile += fmt.Sprintf(`(_%s%d%s)?`, strings.ToUpper(streamType), videoChannel, strings.ToUpper(lang))
					}
				}
			}
		}
	}
	query = `SELECT lang FROM subtitles WHERE contentId=?`
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	rows, err = stmt.Query(*jt.ContentId)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute query %s with (%d): %s", query, *jt.ContentId, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	reSubStr := ``
	subsFound := false
	for rows.Next() {
		var lang string
		err = rows.Scan(&lang)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows for query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		reSubStr += fmt.Sprintf(`(_SUB0%s)?`, strings.ToUpper(lang))
		subsFound = true
	}
	if subsFound == true {
		if likeProfile != "" {
			likeProfileSubBurned = `^` + likeProfile + reSubStr + `$`
		}
	}
	likeProfile = `^` + likeProfile + `$`

	log.Printf("likeProfile is %s", likeProfile)
	log.Printf("likeProfileSubBurned is %s", likeProfileSubBurned)
	query = `SELECT profileId, name FROM profiles WHERE broadcaster=?`
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	rows, err = stmt.Query(strings.ToUpper(*jt.Broadcaster))
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot execute query %s with (%s): %s", query, *jt.Broadcaster, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	profileMatch := -1
	profileNameSelected := ``
	reProfile := regexp.MustCompile(likeProfile)
	reProfileSubBurned := regexp.MustCompile(likeProfileSubBurned)
	for rows.Next() {
		var profileId int
		var name string
		err = rows.Scan(&profileId, &name)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan rows for query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		log.Printf("name is %s", name)
		if reProfile.Match([]byte(name)) == true || (likeProfileSubBurned != "" && reProfileSubBurned.Match([]byte(name)) == true) {
			log.Printf("profile %s (%d) match", name, profileId)
			if len(name) > len(profileNameSelected) {
				profileNameSelected = name
				profileMatch = profileId
			}
		}
	}

	if profileMatch == -1 {
		errStr := `XX There is no profile matching the transcoding request`
		log.Printf(errStr)
		jsonStr := `{"error":"There is no profile matching the transcoding request"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	query = `SELECT p.profileId FROM contentsProfiles AS cp LEFT JOIN profiles AS p ON cp.profileId=p.profileId WHERE contentId=? AND broadcaster=?`
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	currentProfileId := -1
	err = stmt.QueryRow(*jt.ContentId, *jt.Broadcaster).Scan(&currentProfileId)
	if err != nil && err != sql.ErrNoRows {
		errStr := fmt.Sprintf("XX Cannot scan rows for query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	if currentProfileId != -1 {
		query = `DELETE FROM contentsProfiles WHERE contentId=? AND profileId=?`
		stmt, err = db.Prepare(query)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		defer stmt.Close()
		_, err = stmt.Exec(*jt.ContentId, currentProfileId)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot execute query %s with (%s): %s", query, *jt.Broadcaster, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
	}

	log.Printf("profileMatch is %d", profileMatch)
	m := map[string]interface{}{}
	m["profileId"] = float64(profileMatch)
	err = transcode(w, r, m, *jt.ContentId)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot transcode contentId %d with profileId %d: %s", *jt.ContentId, profileMatch, err)
		log.Printf(errStr)
		jsonStr := fmt.Sprintf(`{"error":"Cannot transcode contentId %d with profileId %d: %s"}`, *jt.ContentId, profileMatch, err)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	return
}

/* TOOLS */

func getVideoFileInformations(filename string) (vfi VideoFileInfo, err error) {
	vfi.Stat, err = os.Stat(filename)
	if err != nil {
		return
	}

	// Get FFmpeg informations
	ffmpegArgs := []string{"-i", filename}
	cmd := exec.Command(ffmpegPath, ffmpegArgs...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return
	}
	err = cmd.Start()
	if err != nil {
		return
	}
	re, err := regexp.Compile("Duration: *([0-9]{2}:[0-9]{2}:[0-9]{2})\\.[0-9]{2}, *start: *[0-9\\.]*, *bitrate: *([0-9]*) kb/s")
	if err != nil {
		return
	}
	b := make([]byte, 1024)
	var s string
	for {
		bytesRead, err := stderr.Read(b)
		if err != nil {
			break
		}
		s += string(b[:bytesRead])
	}
	_ = cmd.Wait()
	log.Printf("s = %s", s)
	matches := re.FindAllStringSubmatch(s, -1)
	if matches != nil && matches[0] != nil {
		vfi.Duration = matches[0][1]
		vfi.Bitrate = matches[0][2]
	}
	//Stream #0:0: Video: mpeg2video (4:2:2), yuv422p(tv, bt709), 1920x1080 [SAR 1:1 DAR 16:9], 50000 kb/s, 25 fps, 25 tbr, 25 tbn, 50 tbc
	//Stream #0:0(eng): Video: prores (apch / 0x68637061), yuv422p10le, 1920x1080, 169090 kb/s, 25 fps, 25 tbr, 12800 tbn, 12800 tbc (default)
	//Stream #0:0(und): Video: h264 (Main) (avc1 / 0x31637661), yuv420p(tv, bt709), 1920x1080 [SAR 1:1 DAR 16:9], 2831 kb/s, 23.98 fps, 23.98 tbr, 90k tbn, 180k tbc (default)
	//Stream #0:0(und): Video: mpeg4 (Simple Profile) (mp4v / 0x7634706D), yuv420p, 1920x1080 [SAR 1:1 DAR 16:9], 3257 kb/s, 25 fps, 25 tbr, 12800 tbn, 25 tbc (default)
	//Stream #0:0[0x1e0]: Video: mpeg2video (Main), yuv420p(tv, bt470bg), 720x576 [SAR 64:45 DAR 16:9], 15000 kb/s, 25 fps, 25 tbr, 90k tbn, 50 tbc
	//Stream #0:0[0x3e8]: Video: h264 (Main) ([27][0][0][0] / 0x001B), yuv420p, 720x576 [SAR 512:351 DAR 640:351], 25 fps, 25 tbr, 90k tbn, 50 tbc
	//Stream #0:0(eng): Video: h264 (Constrained Baseline) (avc1 / 0x31637661), yuv420p, 854x480 [SAR 1:1 DAR 427:240], 1569 kb/s, 29.97 fps, 29.97 tbr, 30k tbn, 59.94 tbc (default)
	//Stream #0:0(und): Video: h264 (Constrained Baseline) (avc1 / 0x31637661), yuv420p, 854x480 [SAR 1:1 DAR 427:240], 1355 kb/s, 25 fps, 25 tbr, 12800 tbn, 50 tbc (default)
	//Stream #0:0(eng): Video: h264 (Constrained Baseline) (avc1 / 0x31637661), yuv420p, 426x240 [SAR 1:1 DAR 71:40], 395 kb/s, 29.97 fps, 29.97 tbr, 11988 tbn, 59.94 tbc (default)
	//Stream #0:0[0x3e8]: Video: h264 (Main) ([27][0][0][0] / 0x001B), yuv420p, 720x576 [SAR 64:45 DAR 16:9], 25 fps, 25 tbr, 90k tbn
	//Stream #0:0: Video: mpeg2video (4:2:2), yuv422p(tv, unknown/bt470bg/bt470bg), 720x608 [SAR 152:135 DAR 4:3], 50000 kb/s, 25 fps, 25 tbr, 25 tbn, 50 tbc

	reVideo, err := regexp.Compile(`Stream\s#\d+:(?P<track>\d+)(?:\[[0-9a-fx]+\])?(?:\((?P<lang>\w+)\))?:\sVideo:\s(?P<codec>\w+)(?:\s\((?P<codecProfile>[A-Za-z0-9:\s]+)\))?(?:\s\((?P<codecInfo>[A-Za-z0-9\[\]]+)\s\/\s[0-9a-fA-FxX]+\))?,\s\w+(?:\([^\)]*\))?,\s(?P<width>\d+)x(?P<height>\d+)[^,]*(?:,\sSAR\s\d+:\d+\sDAR\s\d+:\d+)?(?:,\s(?P<bitrate>\d+)\skb\/s)?(?:,\s(?P<fps>[0-9\.]+)\sfps)?`)

	//reVideo, err := regexp.Compile("Stream #[0-9]:([0-9])(\\(?[a-zA-Z]*?\\)?)\\[?[0-9a-fx]*?\\]?: Video: ([a-z0-9]*) \\(?([A-Za-z0-9: ]*?)\\)? \\(?([a-z0-9\\[\\]]*?) ?\\/? ?[0-9a-fA-Fx]*?\\)?, [a-z0-9]*\\(?[^\\)]*?\\)?, *([0-9]*)x([0-9]*)[^,]*, ?([0-9]*?) ?k?b?/?s?,? ([0-9\\.]*) fps.*")
	if err != nil {
		return
	}
	//Stream #0:1: Audio: pcm_s24le, 48000 Hz, 1 channels, s32 (24 bit), 1152 kb/s
	//Stream #0:1[0x3e9](fra): Audio: mp2 ([3][0][0][0] / 0x0003), 48000 Hz, stereo, s16p, 185 kb/s
	//Stream #0:0(eng): Audio: aac (mp4a / 0x6134706D), 48000 Hz, stereo, fltp, 127 kb/s (default)
	//Stream #0:0(und): Audio: aac (LC) (mp4a / 0x6134706D), 48000 Hz, stereo, fltp, 109 kb/s (default)
	//Stream #0:1[0x3e9](fra): Audio: mp2 ([3][0][0][0] / 0x0003), 48000 Hz, stereo, s16p, 192 kb/s
	//Stream #0:2[0x3ea](eng): Audio: mp2 ([3][0][0][0] / 0x0003), 48000 Hz, stereo, s16p, 192 kb/s

	reAudio, err := regexp.Compile("Stream #[0-9]:([0-9])\\[?[0-9a-fx]*?\\]?\\(?([a-z]*?)\\)?: Audio: ([0-9a-zA-Z_]*) *\\(?[A-Za-z0-9]*?\\)? ?\\(?([a-z0-9\\[\\]]*?) ?\\/? ?[0-9a-fA-Fx]*?\\)?, ([0-9]*) Hz, [^,]*, [^,]*, ([0-9]*) kb/s.*")
	if err != nil {
		return
	}
	matches = reVideo.FindAllStringSubmatch(s, -1)
	for _, v := range matches {
		var vs VideoStream
		vs.Id = v[1]
		if v[2] == "" {
			vs.Language = "eng"
		} else {
			vs.Language = strings.Replace(strings.Replace(strings.Replace(strings.Replace(v[2], "(", "", -1), "[", "", -1), ")", "", -1), "]", "", -1)
		}
		vs.Codec = v[3]
		vs.CodecProfile = v[4]
		vs.CodecInfo = v[5]
		vs.Width = v[6]
		vs.Height = v[7]
		if v[8] == "" {
			vs.Bitrate = vfi.Bitrate
		} else {
			vs.Bitrate = v[8]
		}
		vs.Fps = v[9]
		vfi.VideoStreams = append(vfi.VideoStreams, vs)
	}
	matches = reAudio.FindAllStringSubmatch(s, -1)
	for _, v := range matches {
		var as AudioStream
		as.Id = v[1]
		if v[2] == "" {
			as.Language = "eng"
		} else {
			as.Language = strings.Replace(strings.Replace(strings.Replace(strings.Replace(v[2], "(", "", -1), "[", "", -1), ")", "", -1), "]", "", -1)
		}
		as.Codec = v[3]
		as.CodecInfo = v[4]
		as.Frequency = v[5]
		as.Bitrate = v[6]
		vfi.AudioStreams = append(vfi.AudioStreams, as)
	}

	log.Printf("ffmpeg info struct is %+v", vfi)

	return
}

func packageContents(contentUuids []ContentsUuid) (errSave error) {
	var stmt *sql.Stmt
	var err error
	log.Printf("contentUuids=%#v", contentUuids)
	db := database.OpenDb()
	defer db.Close()
	for _, cu := range contentUuids {
		query := "SELECT filename FROM assets WHERE contentId=?"
		stmt, err = db.Prepare(query)
		if err != nil {
			errSave = err
			log.Printf("XX Cannot prepare query %s: %s", query, err)
			continue
		}
		defer stmt.Close()
		var rows *sql.Rows
		rows, err = stmt.Query(cu.ContentId)
		if err != nil {
			errSave = err
			log.Printf("XX Cannot query %s with (%d): %s", query, cu.ContentId, err)
			continue
		}
		defer rows.Close()
		var filenames []string
		for rows.Next() {
			var filename string
			err = rows.Scan(&filename)
			if err != nil {
				errSave = err
				log.Printf("XX Cannot scan rows for query %s: %s", query, err)
				continue
			}
			filenames = append(filenames, filename)
		}
		log.Printf("cu is %#v", cu)
		log.Printf("filename to package: %#v", filenames)
		var uspPackageArgs []string
		uspPackageArgs = append(uspPackageArgs, path.Dir(filenames[0]))
		uspPackageArgs = append(uspPackageArgs, cu.Uuid+`.ism`)
		uspPackageArgs = append(uspPackageArgs, filenames...)
		cmd := exec.Command(uspPackagePath, uspPackageArgs...)
		log.Printf("-- Starting command %s %v", uspPackagePath, uspPackageArgs)
		err = cmd.Start()
		if err != nil {
			errSave = err
			log.Printf("XX Cannot start command %s %v: %s", uspPackagePath, uspPackageArgs, err)
			continue
		}
		err = cmd.Wait()
		if err != nil {
			errSave = err
			log.Printf("XX Error while packaging with %s %v: %s", uspPackagePath, uspPackageArgs, err)
			continue
		}
	}

	return errSave
}

func uuidToContentId(uuid string) (contentId int, err error) {
	db := database.OpenDb()
	defer db.Close()

	query := "SELECT contentId FROM contents WHERE uuid=?"
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		log.Printf("XX Cannot prepare query %s: %s", query, err)
		return
	}
	defer stmt.Close()
	err = stmt.QueryRow(uuid).Scan(&contentId)
	if err != nil {
		log.Printf("XX Cannot query row %s with query %s: %s", uuid, query, err)
		return
	}

	return
}

func getSubtitles(url string, dest string) (err error) {
	var resp *http.Response
	log.Printf("fetch subtitle at url %s to %s", url, dest)
	resp, err = http.Get(url)
	if err != nil {
		tools.LogOnError(err, "Failed to GET url %s", url)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Printf("HTTP(S) transfer error with url %s, HTTP Status Code is %d: %s", url, resp.StatusCode, resp.Body)
		err = errors.New("HTTP(S) transfer error with url %s, HTTP Status Code is %d")
		return
	}
	var f *os.File
	f, err = os.Create(dest)
	if err != nil {
		tools.LogOnError(err, "Failed to GET url %s", url)
		return
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return
	}

	return
}

func transcode(w http.ResponseWriter, r *http.Request, m map[string]interface{}, contentId int) (err error) {
	err = nil
	db := database.OpenDb()
	defer db.Close()
	query := "SELECT cmdLine FROM presets WHERE profileId=?"
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(m["profileId"].(float64))
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()
	var errMsg []string
	profilesParameters := map[string]string{}
	for rows.Next() {
		var cmdLine string
		err = rows.Scan(&cmdLine)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		var re *regexp.Regexp
		regexpStr := "%([a-z]+[a-zA-Z]*)%"
		re, err = regexp.Compile(regexpStr)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot compile regexp %s: %s", regexpStr, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		matches := re.FindAllStringSubmatch(cmdLine, -1)
		for _, v := range matches {
			if m[v[1]] == nil || m[v[1]].(string) == "" {
				errMsg = append(errMsg, "'"+v[1]+"' is missing")
			} else {
				profilesParameters[v[1]] = m[v[1]].(string)
			}
		}
	}
	if errMsg != nil {
		w.Write([]byte(`{"error":"` + strings.Join(errMsg, ",") + `"}`))
		err = fmt.Errorf("%s", errMsg)
		return
	}

	query = "SELECT p.profileId FROM contentsProfiles AS cp LEFT JOIN profiles AS p ON cp.profileId=p.profileId WHERE contentId=? AND p.name LIKE '%_USP%'"
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	var contentProfileId string
	err = stmt.QueryRow(contentId).Scan(&contentProfileId)
	if err != nil {
		contentProfileId = "-1"
	}
	stmt.Close()

	query = "SELECT filename,uuid FROM contents WHERE contentId=?"
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	var contentFilename string
	var uuid string
	err = stmt.QueryRow(contentId).Scan(&contentFilename, &uuid)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	baseContentFilename := path.Base(contentFilename)
	extension := filepath.Ext(baseContentFilename)
	contentFilenameBase := baseContentFilename[0 : len(baseContentFilename)-len(extension)]

	query = fmt.Sprintf("SELECT presetId,CONCAT('%s/origin/vod/%s/%s_',`name`),presetIdDependance,doAnalyze FROM presets AS pr WHERE pr.profileId=? ORDER BY pr.presetIdDependance", encodedBasePath, contentFilenameBase, uuid)
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()
	rows, err = stmt.Query(m["profileId"].(float64))
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer rows.Close()

	var doAnalyze string
	var presetId int
	var outputFilename string
	var presetIdDependance *string
	var assetIdDependance *string
	assetIdDependance = nil
	presetToAssetIdMap := make(map[string]*int)
	var assetIds []int64

	query = "DELETE FROM assets WHERE contentId=? AND presetId=?"
	stmt, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt.Close()

	query = "INSERT INTO assets (`contentId`,`presetId`,`assetIdDependance`,`filename`,`doAnalyze`,`createdAt`) VALUES (?,?,?,?,?,NULL)"
	var stmt2 *sql.Stmt
	stmt2, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt2.Close()

	query = "DELETE FROM profilesParameters WHERE profileId=? AND assetId=? AND parameter=?"
	var stmt3 *sql.Stmt
	stmt3, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt3.Close()

	query = "INSERT INTO profilesParameters (`profileId`,`assetId`,`parameter`,`value`,`createdAt`) VALUES (?,?,?,?,NOW())"
	var stmt4 *sql.Stmt
	stmt4, err = db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt4.Close()

	query = "INSERT INTO contentsProfiles (`contentId`, `profileId`) VALUES (?,?)"
	stmt5, err := db.Prepare(query)
	if err != nil {
		errStr := fmt.Sprintf("XX Cannot prepare query %s: %s", query, err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}
	defer stmt5.Close()
	_, err = stmt5.Exec(contentId, m["profileId"].(float64))
	if err != nil {
		errStr := fmt.Sprintf("XX Error during query execution %s with (%d,%f): %s", query, contentId, m["profileId"].(float64), err)
		log.Printf(errStr)
		jsonStr := `{"error":"` + err.Error() + `"}`
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(jsonStr))
		return
	}

	for rows.Next() {
		err = rows.Scan(&presetId, &outputFilename, &presetIdDependance, &doAnalyze)
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot scan query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		if presetIdDependance != nil {
			assetIdDependance = new(string)
			presetIdsDependance := strings.Split(*presetIdDependance, `,`)
			for _, p := range presetIdsDependance {
				if presetToAssetIdMap[p] != nil {
					*assetIdDependance += strconv.Itoa(*presetToAssetIdMap[p]) + `,`
				}
			}
			*assetIdDependance = (*assetIdDependance)[:len(*assetIdDependance)-1]
		}

		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		rStr := r.Intn(999)
		random := fmt.Sprintf("%d", rStr)
		outputFilename = strings.Replace(outputFilename, `%RANDOM%`, random, -1)
		contentIdStr := strconv.Itoa(contentId)
		outputFilename = strings.Replace(outputFilename, `%CONTENTID%`, contentIdStr, -1)
		outputFilename = strings.Replace(outputFilename, `%CONTENTEXT%`, path.Ext(contentFilename), -1)
		outputFilename = strings.Replace(outputFilename, `%CONTENTPROFILEID%`, contentProfileId, -1)
		if strings.Contains(outputFilename, `%SOURCE%`) {
			outputFilename = contentFilename
		}
		log.Printf("outputFilename is %s", outputFilename)
		var result sql.Result
		_, err = stmt.Exec(contentId, presetId)
		if err != nil {
			errStr := fmt.Sprintf("XX Error during query execution %s with (%d,%d): %s", query, contentId, presetId, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		if assetIdDependance == nil {
			result, err = stmt2.Exec(contentId, presetId, nil, outputFilename, doAnalyze)
		} else {
			result, err = stmt2.Exec(contentId, presetId, *assetIdDependance, outputFilename, doAnalyze)
		}
		if err != nil {
			errStr := fmt.Sprintf("XX Error during query execution %s with %s: %s", query, m["profileId"].(float64), err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		var assetId int64
		assetId, err = result.LastInsertId()
		if err != nil {
			errStr := fmt.Sprintf("XX Cannot get last insert ID with query %s: %s", query, err)
			log.Printf(errStr)
			jsonStr := `{"error":"` + err.Error() + `"}`
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonStr))
			return
		}
		for k, v := range profilesParameters {
			_, err = stmt3.Exec(m["profileId"].(float64), assetId, k)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot Execute query %s with (%s, %d, %s): %s", query, m["profileId"].(float64), assetId, k, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
			_, err = stmt4.Exec(m["profileId"].(float64), assetId, k, v)
			if err != nil {
				errStr := fmt.Sprintf("XX Cannot Execute query %s with (%s, %d, %s, %s): %s", query, m["profileId"].(float64), assetId, k, v, err)
				log.Printf(errStr)
				jsonStr := `{"error":"` + err.Error() + `"}`
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(jsonStr))
				return
			}
		}
		assetIds = append(assetIds, assetId)
		v := new(int)
		*v = int(assetId)
		presetToAssetIdMap[strconv.Itoa(presetId)] = v
	}
	database.DbSetContentState(db, contentId, "scheduled")

	jsonAnswer := `{"assetsId":[`
	for _, a := range assetIds {
		log.Printf("assetId is: %#v", a)
		jsonAnswer += strconv.FormatInt(a, 10) + `,`
	}
	jsonAnswer = jsonAnswer[:len(jsonAnswer)-1] + `]}`

	w.Write([]byte(jsonAnswer))

	return
}

func md5HashToContentId(md5Hash string) (contentId int, err error) {
	db := database.OpenDb()
	defer db.Close()

	query := "SELECT contentId FROM contents WHERE md5Hash=?"
	var stmt *sql.Stmt
	stmt, err = db.Prepare(query)
	if err != nil {
		log.Printf("XX Cannot prepare query %s: %s", query, err)
		return
	}
	defer stmt.Close()
	err = stmt.QueryRow(md5Hash).Scan(&contentId)
	if err != nil {
		log.Printf("XX Cannot query row %s with query %s: %s", md5Hash, query, err)
		return
	}

	return
}