package main

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"log"
	"strconv"
	"strings"
	"time"
	"io/ioutil"
	"encoding/json"
	"net/http"
	"database/sql"
        _ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
)

const dbDsn = "pfscheduler:DgE4jfVh65jD34dF@tcp(10.91.83.18:3306)/video_encoding"
const ffmpegPath = "/usr/bin/ffmpeg"
const uspPackagePath = "/usr/local/bin/usp_package.sh"
const outputBasePath = "/space/videos/encoded"

type Contents struct {
  ProfileId	*int	`json:"profileId"`
  ContentId	*int	`json:"contentId"`
  Uuid		*string	`json:"uuid"`
  Filename	*string	`json:"filename"`
  State		*string	`json:"state"`
  Size		*int	`json:"size"`
  Duration	*string	`json:"duration"`
  Drm		*string	`json:"drm"`
  CreatedAt	*string	`json:"createdAt"`
  UpdatedAt	*string	`json:"updatedAt"`
}

type Assets struct {
  AssetId	*int	`json:"assetId"`
  ContentId	*int	`json:"contentId"`
  PresetId	*int	`json:"presetId"`
  AssetIdDependance	*int	`json:"assetIdDependance"`
  Filename	*string	`json:"filename"`
  State		*string	`json:"state"`
  CreatedAt	*string	`json:"createdAt"`
  UpdatedAt	*string	`json:"updatedAt"`
}

type Encoders struct {
  EncoderId	*int	`json:"encoderId"`
  Hostname	*string	`json:"hostname"`
  ActiveTasks   *int	`json:"activeTasks"`
  Load1		*float64	`json:"load1"`
  UpdatedAt	*string	`json:"updatedAt"`
}

type FFmpegLogs struct {
  AssetId	*int	`json:"assetId"`
  Log		*string	`json:"log"`
}

type FFmpegProgress struct {
  AssetId	*int	`json:"assetId"`
  Frame		*int	`json:"frame"`
  Fps		*int	`json:"fps"`
  Q		*int	`json:"q"`
  Size		*int	`json:"size"`
  Elapsed	*string	`json:"elapsed"`
  Bitrate	*float64	`json:"bitrate"`
}

type Presets struct {
  PresetId		*int	`json:"presetId"`
  ProfileId		*int	`json:"profileId"`
  Name			*string	`json:"name"`
  Type			*string	`json:"type"`
  Language		*string	`json:"language"`
  Codec			*string	`json:"codec"`
  CodecProfile		*string	`json:"codecProfile"`
  CodecLevel		*string	`json:"codecLevel"`
  Width			*int	`json:"width"`
  Height		*int	`json:"height"`
  Bitrate		*int	`json:"bitrate"`
  Gop			*int	`json:"gop"`
  FFmpegParameters	*string	`json:"ffmpegParameters"`
  CreatedAt		*string	`json:"createdAt"`
  UpdatedAt		*string	`json:"updatedAt"`
}

type Profiles struct {
  ProfileId	*int	`json:"profileId"`
  Name		*string	`json:"name"`
  CreatedAt	*string	`json:"createdAt"`
  UpdatedAt	*string	`json:"updatedAt"`
}

type Streams struct {
  ContentId	*int	`json:"contentId"`
  MapId		*int	`json:"mapId"`
  Type		*string	`json:"type"`
  Language	*string	`json:"language"`
  Codec		*string	`json:"codec"`
  CodecInfo	*string	`json:"codecInfo"`
  CodecProfile	*string	`json:"codecProfile"`
  Bitrate	*int	`json:"bitrate"`
  Frequency	*int	`json:"frequency"`
  Width		*int	`json:"width"`
  Height	*int	`json:"height"`
  Fps		*int	`json:"fps"`
  CreatedAt	*string	`json:"createdAt"`
  UpdatedAt	*string	`json:"updatedAt"`
}

type JsonCreateContent struct {
  Filename	*string	`json:"filename"`
  Drm		*string	`json:"drm"`
  ProfileId	*int	`json:"profileId"`
}

type JsonPackageContent struct {
  ContentId	*[]int	`json:"contentId"`
}

type VideoFileInfo struct {
  Stat		os.FileInfo
  Duration	string
  Bitrate	string
  VideoStreams	[]VideoStream
  AudioStreams	[]AudioStream
}

type VideoStream struct {
  Id		string
  Language	string
  Codec		string
  CodecInfo	string
  CodecProfile  string
  Bitrate	string
  Width		string
  Height	string
  Fps		string
}

type AudioStream struct {
  Id		string
  Language	string
  Codec		string
  CodecInfo	string
  Bitrate	string
  Frequency	string
}

type ContentsUuid struct {
  ContentId int
  Uuid string
}

func logOnError(err error, format string, v ...interface{}) {
  format = format + ": %s"
  if err != nil {
    log.Printf(format, v, err)
  }
}

func failOnError(err error, msg string) {
  if err != nil {
    log.Printf("%s: %s", msg, err)
    panic(fmt.Sprintf("%s: %s", msg, err))
  }
}

func openDb() (db *sql.DB) {
  db, err := sql.Open("mysql", dbDsn)
  logOnError(err, "Cannot open database %s", dbDsn)
  err = db.Ping()
  logOnError(err, "Cannot ping database %s", dbDsn)

  return
}

func publishExchange(ch *amqp.Channel, key string, msg string) (err error) {
  log.Printf("-- Sending message '%s' on afsm-encoders queue", msg)
  err = ch.Publish(
    "afsm-encoders",     // exchange
    key, // routing key
    false,  // mandatory
    false,  // immediate
    amqp.Publishing {
      ContentType: "text/plain",
      Body:        []byte(msg),
    })
  failOnError(err, fmt.Sprintf("Failed to publish a message '%s'", msg))

  return
}

// API Handlers
func contentsGetHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  params := mux.Vars(r)
  w.Header().Set("Content-Type", "application/json")
  id := -1
  profileId := -1
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
  db := openDb()
  defer db.Close()

  var query string
  if id >= 0 {
    query = "SELECT * FROM contents WHERE contentId=?"
  } else {
    if profileId >= 0 {
      query = "SELECT * FROM contents WHERE profileId=?"
      id = profileId
    } else {
      query = "SELECT * FROM contents"
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
  var c Contents
  var rows *sql.Rows
  if id == -1 {
    rows, err = stmt.Query()
  } else {
    rows, err = stmt.Query(id)
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
    err = rows.Scan(&c.ContentId, &c.ProfileId, &c.Uuid, &c.Filename, &c.State, &c.Size, &c.Duration, &c.Drm, &c.CreatedAt, &c.UpdatedAt)
    if err != nil {
      errStr := fmt.Sprintf("XX Cannot scan rows result for query %s: %s", query, err)
      log.Printf(errStr)
      jsonStr := `{"error":"` + err.Error() + `"}`
      w.WriteHeader(http.StatusNotFound)
      w.Write([]byte(jsonStr))
      return
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
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer[:len(jsonAnswer)-1] + "]"
  }
  w.Write([]byte(jsonAnswer))
}

func getVideoFileInformations(filename string) (vfi VideoFileInfo, err error) {
  vfi.Stat, err = os.Stat(filename)
  if err != nil {
    return
  }

  // Get FFmpeg informations
  ffmpegArgs := []string{ "-i", filename }
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
  log.Printf("s = %s", s)
  matches := re.FindAllStringSubmatch(s, -1)
  if matches != nil && matches[0] != nil {
    vfi.Duration = matches[0][1]
    vfi.Bitrate = matches[0][2]
  }
  reVideo, err := regexp.Compile("Stream *#[0-9]:([0-9])\\(([a-z]{3})\\): *Video: ([0-9a-zA-Z]*) *\\(([A-Za-z]*)\\) *\\(([a-z0-9]*) *\\/ *[0-9a-fA-Fx]*\\), *[a-z0-9\\(\\)]*, *([0-9]*)x([0-9]*) *\\[.*\\], *([0-9]*) *kb/s, *([0-9\\.]*) *fps, *")
  if err != nil {
    return
  }
  reAudio, err := regexp.Compile("Stream *#[0-9]:([0-9])\\(([a-z]{3})\\): *Audio: *([0-9a-zA-Z]*) *\\(([a-z0-9]*) *\\/ *[0-9a-fA-Fx]*\\), *([0-9]*) *Hz, *[a-z]*, *[a-z]*, *([0-9]*) *kb/s")
  if err != nil {
    return
  }
  matches = reVideo.FindAllStringSubmatch(s, -1)
  for _, v := range matches {
    var vs VideoStream
    vs.Id = v[1]
    vs.Language = v[2]
    vs.Codec = v[3]
    vs.CodecProfile = v[4]
    vs.CodecInfo = v[5]
    vs.Width = v[6]
    vs.Height = v[7]
    vs.Bitrate = v[8]
    vs.Fps = v[9]
    vfi.VideoStreams = append(vfi.VideoStreams, vs)
  }
  matches = reAudio.FindAllStringSubmatch(s, -1)
  for _, v := range matches {
    var as AudioStream
    as.Id = v[1]
    as.Language = v[2]
    as.Codec = v[3]
    as.CodecInfo = v[4]
    as.Frequency = v[5]
    as.Bitrate = v[6]
    vfi.AudioStreams = append(vfi.AudioStreams, as)
  }

  log.Printf("ffmpeg info struct is %+v", vfi)

  return
}

func contentsPostHandler(w http.ResponseWriter, r *http.Request) {
  body, _ := ioutil.ReadAll(r.Body)
  w.Header().Set("Content-Type", "application/json")
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
  if jcc.Drm == nil {
    errMsg = append(errMsg, "'drm' is missing")
  }
  if jcc.ProfileId == nil {
    errMsg = append(errMsg, "'profileId' is missing")
  }
  if errMsg != nil {
    w.Write([]byte(`{"error":"` + strings.Join(errMsg, ",") + `"}`))
    return
  }

  db := openDb()
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

  uuid := uuid.New()
  query := "INSERT INTO contents (`profileId`,`uuid`,`filename`,`size`,`duration`,`drm`,`createdAt`) VALUES (?,?,?,?,?,?,NULL)"
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
  result, err := stmt.Exec(*jcc.ProfileId, uuid, *jcc.Filename, vfi.Stat.Size(), vfi.Duration, *jcc.Drm)
  if err != nil {
    errStr := fmt.Sprintf("XX Error during query execution %s with (%s,%s,%s,%d,%d,%s): %s", query, *jcc.ProfileId, uuid, *jcc.Filename, vfi.Stat.Size(), vfi.Duration, *jcc.Drm, err)
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
    stmt, err = db.Prepare("INSERT INTO streams (`contentId`,`mapId`,`type`,`language`,`codec`,`codecInfo`,`codecProfile`,`bitrate`,`width`,`height`,`fps`,`createdAt`) VALUES (?,?,'video',?,?,?,?,?,?,?,?,NULL)")
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
      errStr := fmt.Sprintf("XX Error during query execution %s with (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s): %s", query, contentId, vs.Id, vs.Language, vs.Codec, vs.CodecInfo, vs.CodecProfile, vs.Bitrate, vs.Width, vs.Height, vs.Fps, err)
      log.Printf(errStr)
      jsonStr := `{"error":"` + err.Error() + `"}`
      w.WriteHeader(http.StatusNotFound)
      w.Write([]byte(jsonStr))
      return
    }
  }
  for _, as := range vfi.AudioStreams {
    stmt, err = db.Prepare("INSERT INTO streams (`contentId`,`mapId`,`type`,`language`,`codec`,`codecInfo`,`bitrate`,`frequency`,`createdAt`) VALUES (?,?,'audio',?,?,?,?,?,NULL)")
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
      errStr := fmt.Sprintf("XX Error during query execution %s with (%s,%s,%s,%s,%s,%s,%s): %s", query, contentId, as.Id, as.Language, as.Codec, as.CodecInfo, as.Bitrate, as.Frequency, err)
      log.Printf(errStr)
      jsonStr := `{"error":"` + err.Error() + `"}`
      w.WriteHeader(http.StatusNotFound)
      w.Write([]byte(jsonStr))
      return
    }
  }

  query = fmt.Sprintf("SELECT presetId,CONCAT('%s','/','%s','_',`name`),presetIdDependance FROM presets AS pr WHERE pr.profileId=? ORDER BY pr.presetIdDependance", outputBasePath, uuid)
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
  rows, err = stmt.Query(jcc.ProfileId)
  if err != nil {
    errStr := fmt.Sprintf("XX Cannot query %s: %s", query, err)
    log.Printf(errStr)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  defer rows.Close()

  var presetId int
  var outputFilename string
  var presetIdDependance *int
  var assetIdDependance *int
  presetToAssetIdMap := make(map[int]*int)
  var assetIds []int64
  for rows.Next() {
    err = rows.Scan(&presetId, &outputFilename, &presetIdDependance)
    if err != nil {
      errStr := fmt.Sprintf("XX Cannot scan query %s: %s", query, err)
      log.Printf(errStr)
      jsonStr := `{"error":"` + err.Error() + `"}`
      w.WriteHeader(http.StatusNotFound)
      w.Write([]byte(jsonStr))
      return
    }
    assetIdDependance = nil
    if presetIdDependance != nil {
      if presetToAssetIdMap[*presetIdDependance] != nil {
        assetIdDependance = presetToAssetIdMap[*presetIdDependance]
      }
    }
    query = "INSERT INTO assets (`contentId`,`presetId`,`assetIdDependance`,`filename`,`createdAt`) VALUES (?,?,?,?,NULL)"
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
    if assetIdDependance == nil {
      result, err = stmt.Exec(contentId, presetId, nil, outputFilename)
    } else {
      result, err = stmt.Exec(contentId, presetId, *assetIdDependance, outputFilename)
    }
    if err != nil {
      errStr := fmt.Sprintf("XX Error during query execution %s with %s: %s", jcc.ProfileId, err)
      log.Printf(errStr)
      jsonStr := `{"error":"` + err.Error() + `"}`
      w.WriteHeader(http.StatusNotFound)
      w.Write([]byte(jsonStr))
      return
    }
    assetId, err := result.LastInsertId()
    if err != nil {
      errStr := fmt.Sprintf("XX Cannot get last insert ID with query %s: %s", query, err)
      log.Printf(errStr)
      jsonStr := `{"error":"` + err.Error() + `"}`
      w.WriteHeader(http.StatusNotFound)
      w.Write([]byte(jsonStr))
      return
    }
    assetIds = append(assetIds, assetId)
    v := new(int)
    *v = int(assetId)
    presetToAssetIdMap[presetId] = v
  }

  jsonAnswer := fmt.Sprintf(`{"contentId":%d,"assetsId":[`, contentId)
  for _, a := range assetIds {
    log.Printf("assetId is: %#v", a)
    jsonAnswer += `{` + strconv.FormatInt(a, 10) + `},`
  }
  jsonAnswer = jsonAnswer[:len(jsonAnswer)-1] + `]}`

  w.Write([]byte(jsonAnswer))
}

func assetsGetHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
  params := mux.Vars(r)
  id := -1
  contentId := -1
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
  db := openDb()
  defer db.Close()

  var query string
  if id >= 0 {
    query = "SELECT * FROM assets WHERE assetId=?"
  } else {
    if contentId >= 0 {
      query = "SELECT * FROM assets WHERE contentId=?"
      id = contentId
    } else {
      query = "SELECT * FROM assets"
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
  var a Assets
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
    err = rows.Scan(&a.AssetId, &a.ContentId, &a.PresetId, &a.AssetIdDependance, &a.Filename, &a.State, &a.CreatedAt, &a.UpdatedAt)
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
  if jsonAnswer != "" {
    jsonAnswer = jsonAnswer[:len(jsonAnswer) - 1]
  }
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer + "]"
  }

  w.Write([]byte(jsonAnswer))
}

func encodersGetHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
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
  db := openDb()
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
  var e Encoders
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
    err = rows.Scan(&e.EncoderId, &e.Hostname, &e.ActiveTasks, &e.Load1, &e.UpdatedAt)
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
  jsonAnswer = jsonAnswer[:len(jsonAnswer) - 1]
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer + "]"
  }
  w.Write([]byte(jsonAnswer))
}

func ffmpegLogsGetHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
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
  db := openDb()
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
  var fl FFmpegLogs
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
  jsonAnswer = jsonAnswer[:len(jsonAnswer) - 1]
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer + "]"
  }
  w.Write([]byte(jsonAnswer))
}

func ffmpegProgressGetHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
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
  db := openDb()
  defer db.Close()

  var query string
  if id == -1 {
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
  var fp FFmpegProgress
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
  jsonAnswer = jsonAnswer[:len(jsonAnswer) - 1]
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer + "]"
  }
  w.Write([]byte(jsonAnswer))
}

func presetsGetHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
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
  db := openDb()
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
  var p Presets
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
    err = rows.Scan(&p.PresetId, &p.ProfileId, &p.Name, &p.Type, &p.Language, &p.Codec, &p.CodecProfile, &p.CodecLevel, &p.Width, &p.Height, &p.Bitrate, &p.Gop, &p.FFmpegParameters, &p.CreatedAt, &p.UpdatedAt)
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
  jsonAnswer = jsonAnswer[:len(jsonAnswer) - 1]
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer + "]"
  }
  w.Write([]byte(jsonAnswer))
}

func profilesGetHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
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
  db := openDb()
  defer db.Close()

  var query string
  if id == -1 {
    query = "SELECT * FROM profiles"
  } else {
    query = "SELECT * FROM profiles WHERE profileId=?"
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
  var p Profiles
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
    err = rows.Scan(&p.ProfileId, &p.Name, &p.CreatedAt, &p.UpdatedAt)
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
  jsonAnswer = jsonAnswer[:len(jsonAnswer) - 1]
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer + "]"
  }
  w.Write([]byte(jsonAnswer))
}

func streamsGetHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
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

  db := openDb()
  defer db.Close()

  var query string
  if id == -1 {
    query = "SELECT * FROM streams"
  } else {
    query = "SELECT * FROM streams WHERE contentId=?"
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
  var s Streams
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
    err = rows.Scan(&s.ContentId, &s.MapId, &s.Type, &s.Language, &s.Codec, &s.CodecInfo, &s.CodecProfile, &s.Bitrate, &s.Frequency, &s.Width, &s.Height, &s.Fps, &s.CreatedAt, &s.UpdatedAt)
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
  jsonAnswer = jsonAnswer[:len(jsonAnswer) - 1]
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer + "]"
  }
  w.Write([]byte(jsonAnswer))
}

func packagePostHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  body, _ := ioutil.ReadAll(r.Body)
  w.Header().Set("Content-Type", "application/json")
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
  db := openDb()
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

func dbSetContentState(db *sql.DB, contentId int, state string) (err error) {
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

func packageContents(contentUuids []ContentsUuid) (errSave error) {
  var stmt *sql.Stmt
  var err error
  log.Printf("%#v", contentUuids)
  db := openDb()
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
    log.Printf("uuid = %s, filename to package: %#v", cu.Uuid, filenames)
    var uspPackageArgs []string
    uspPackageArgs = append(uspPackageArgs, outputBasePath)
    uspPackageArgs = append(uspPackageArgs, cu.Uuid + `.ism`)
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

func sendEncodingTasks(ch *amqp.Channel, queueName string) {
  ticker := time.NewTicker(time.Second * 10)
  log.Printf("-- Starting encoding tasks sender thread")
  go func() {
    for _ = range ticker.C {
      // Send encoding task to the queue
      db := openDb()
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
        var assetIdDependance *int
        err = rows.Scan(&assetId, &contentId, &assetIdDependance)
        log.Printf("assetIdDependance %v", assetIdDependance)
        if assetIdDependance != nil {
          query := "SELECT state FROM assets WHERE assetId=?"
          stmt, err := db.Prepare(query)
          if err != nil {
            log.Printf("XX Cannot prepare query %s: %s", query, err)
            continue
          }
          defer stmt.Close()
          var state string
          err = stmt.QueryRow(*assetIdDependance).Scan(&state)
          if err != nil {
            log.Printf("XX Cannot query row %d with query %s: %s", *assetIdDependance, query, err)
            continue
          }
          if state == "ready" {
            assetIds = append(assetIds, assetId)
            contentIdsMap[contentId] = true
          }
        } else {
          assetIds = append(assetIds, assetId)
          contentIdsMap[contentId] = true
        }
      }

      for _, assetId := range assetIds {
        query := "SELECT hostname FROM encoders WHERE activeTasks < 4 GROUP BY load1 DESC LIMIT 1"
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
          log.Printf("XX Nor more slots available on encoders")
          break
        }
        log.Printf("-- Encoder '%s' will take the task assetId %d", hostname, assetId)
        body := fmt.Sprintf(`{ "hostname": "%s", "assetId": %d }`, hostname, assetId)
        publishExchange(ch, queueName, body)
      }

      for contentId, _ := range contentIdsMap {
        dbSetContentState(db, contentId, "processing")
      }

      query = "UPDATE contents SET state='packaging' WHERE contentId NOT IN (SELECT contentId FROM assets WHERE state <> 'ready') AND contents.uspPackage='enabled'"
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
      go packageContents(contentUuids)

      db.Close()
    }
  }()
}

func main() {
  conn, err := amqp.Dial("amqp://p-afsmsch-001.afrostream.tv/")
  failOnError(err, "Failed to connect to RabbitMQ")
  defer conn.Close()

  ch, err := conn.Channel()
  failOnError(err, "Failed to open a channel")
  defer ch.Close()

  err = ch.ExchangeDeclare(
    "afsm-encoders",   // name
    "fanout", // type
    true,     // durable
    false,    // auto-deleted
    false,    // internal
    false,    // no-wait
    nil,      // arguments
  )
  failOnError(err, "Failed to declare an exchange")

  q, err := ch.QueueDeclare(
    "",
    false,
    false,
    true,
    false,
    nil,
  )
  failOnError(err, "Failed to declare a queue")

  err = ch.QueueBind(
    q.Name, // queue name
    "",     // routing key
    "afsm-encoders", // exchange
    false,
    nil,
  )
  failOnError(err, "Failed to bind a queue")

  sendEncodingTasks(ch, q.Name)

  r := mux.NewRouter()
  r.HandleFunc("/api/contents", contentsGetHandler).Methods("GET")
  r.HandleFunc("/api/contents", contentsPostHandler).Methods("POST")
  r.HandleFunc("/api/contents/{id:[0-9]+}", contentsGetHandler).Methods("GET")
  r.HandleFunc("/api/contents/{id:[0-9]+}/streams", streamsGetHandler).Methods("GET")
  r.HandleFunc("/api/contents/{contentId:[0-9]+}/assets", assetsGetHandler).Methods("GET")
  r.HandleFunc("/api/assets", assetsGetHandler).Methods("GET")
  r.HandleFunc("/api/assets/{id:[0-9]+}", assetsGetHandler).Methods("GET")
  r.HandleFunc("/api/assets/{id:[0-9]+}/ffmpegLogs/current", ffmpegLogsGetHandler).Methods("GET")
  r.HandleFunc("/api/assets/{id:[0-9]+}/ffmpegProgress/current", ffmpegProgressGetHandler).Methods("GET")
  r.HandleFunc("/api/encoders", encodersGetHandler).Methods("GET")
  r.HandleFunc("/api/encoders/{id:[0-9]+}", encodersGetHandler).Methods("GET")
  r.HandleFunc("/api/ffmpegLogs", ffmpegLogsGetHandler).Methods("GET")
  r.HandleFunc("/api/ffmpegProgress", ffmpegProgressGetHandler).Methods("GET")
  r.HandleFunc("/api/presets", presetsGetHandler).Methods("GET")
  r.HandleFunc("/api/presets/{id:[0-9]+}", presetsGetHandler).Methods("GET")
  r.HandleFunc("/api/profiles", profilesGetHandler).Methods("GET")
  r.HandleFunc("/api/profiles/{id:[0-9]+}", profilesGetHandler).Methods("GET")
  r.HandleFunc("/api/profiles/{profileId:[0-9]+}/contents", contentsGetHandler).Methods("GET")
  r.HandleFunc("/api/profiles/{profileId:[0-9]+}/presets", presetsGetHandler).Methods("GET")
  r.HandleFunc("/api/streams", streamsGetHandler).Methods("GET")
  r.HandleFunc("/api/package", packagePostHandler).Methods("POST")

  http.Handle("/", r)
  http.ListenAndServe(":4000", nil)

  body := `{ "hostname": "p-afsmenc-001", "assetId": 1 }`
  publishExchange(ch, q.Name, body)

  log.Printf(" [*] Sending message")
}
