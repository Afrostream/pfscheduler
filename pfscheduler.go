package main

import (
	"fmt"
        "math/rand"
	"os"
	"os/exec"
	"regexp"
	"log"
	"strconv"
	"strings"
        "path"
        "path/filepath"
	"time"
	"io/ioutil"
	"encoding/json"
	"net/http"
	"database/sql"
        _ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"github.com/gorilla/mux"
//        "github.com/gorilla/handlers"
	"github.com/pborman/uuid"
)

const dbDsn = "pfscheduler:DgE4jfVh65jD34dF@tcp(10.91.83.18:3306)/video_encoding"
const ffmpegPath = "/usr/bin/ffmpeg"
const uspPackagePath = "/usr/local/bin/usp_package.sh"
const outputBasePath = "/space/videos/encoded"

type ContentsStreamsPutJson struct {
  Language	*string	`json:"language"`
}

type Contents struct {
  ProfileIds	[]int	`json:"profilesIds"`
  ContentId	*int	`json:"contentId"`
  Uuid		*string	`json:"uuid"`
  Md5Hash	*string `json:"md5Hash"`
  Filename	*string	`json:"filename"`
  State		*string	`json:"state"`
  Size		*int	`json:"size"`
  Duration	*string	`json:"duration"`
  UspPackage	*string `json:"uspPackage"`
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
  DoAnalyze     *string `json:"doAnalyze"`
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
  PresetIdDependance	*string	`json:"presetIdDependance"`
  Name			*string	`json:"name"`
  Type			*string	`json:"type"`
  DoAnalyze		*string `json:"doAnalyze"`
  CmdLine		*string	`json:"cmdLine"`
  CreatedAt		*string	`json:"createdAt"`
  UpdatedAt		*string	`json:"updatedAt"`
}

type Profiles struct {
  ProfileId	*int	`json:"profileId"`
  Name		*string	`json:"name"`
  CreatedAt	*string	`json:"createdAt"`
  UpdatedAt	*string	`json:"updatedAt"`
}

type ContentsStreams struct {
  ContentsStreamId	*int	`json:"contentsStreamId"`
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

type AssetsStreams struct {
  AssetId	*int	`json:"assetId"`
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

type ProfilesParameters struct {
  ProfileParameterId	*int	`json:"presetParameterId"`
  ProfileId	*int	`json:"presetId"`
  Parameter	*string	`json:"parameter"`
  Value		*string	`json:"value"`
  CreatedAt	*string	`json:"createAt"`
  UpdatedAt	*string	`json:"updatedAt"`
}

type JsonCreateContent struct {
  Filename	*string	`json:"filename"`
}

type JsonPackageContent struct {
  ContentId	*[]int	`json:"contentId"`
}

type JsonSetSubtitles struct {
  S		[]Sub	`json:"subtitles"`
}

type Sub struct {
  ProfileId	int	`json:"profileId"`
  Language	string	`json:"lang"`
  Url		string	`json:"url"`
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

func uuidToContentId(uuid string) (contentId int, err error) {
  db := openDb()
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

func md5HashToContentId(md5Hash string) (contentId int, err error) {
  db := openDb()
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

// API Handlers
func contentsGetHandler(w http.ResponseWriter, r *http.Request) {
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
  db := openDb()
  defer db.Close()

  var query string
  if md5Hash != ""  {
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
    var c Contents
    err = rows.Scan(&c.ContentId, &c.Uuid, &c.Md5Hash, &c.Filename, &c.State, &c.Size, &c.Duration, &c.UspPackage, &c.Drm, &c.CreatedAt, &c.UpdatedAt)
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
    rows2, err = stmt.Query(c.ContentId)
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

  reVideo, err := regexp.Compile("Stream #[0-9]:([0-9])(\\(?[a-zA-Z]*?\\)?)\\[?[0-9a-fx]*?\\]?: Video: ([a-z0-9]*) \\(?([A-Za-z0-9: ]*?)\\)? \\(?([a-z0-9\\[\\]]*?) ?\\/? ?[0-9a-fA-Fx]*?\\)?, [a-z0-9]*\\(?[^\\)]*?\\)?, *([0-9]*)x([0-9]*)[^,]*, ?([0-9]*?) ?k?b?/?s?,? ([0-9\\.]*) fps.*")
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

func contentsPostHandler(w http.ResponseWriter, r *http.Request) {
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

  // Compute md5sum of file datas
  md5sum, err := exec.Command(`/usr/bin/md5sum`, *jcc.Filename).Output()
  if err != nil  {
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

func transcode(w http.ResponseWriter, r *http.Request, m map[string]interface{}, contentId int) {
  db := openDb()
  defer db.Close()
  query := "SELECT cmdLine FROM presets WHERE profileId=?"
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
      if m[v[1]].(string) == "" {
        errMsg = append(errMsg, "'" + v[1] + "' is missing")
      } else {
        profilesParameters[v[1]] = m[v[1]].(string)
      }
    }
  }
  if errMsg != nil {
    w.Write([]byte(`{"error":"` + strings.Join(errMsg, ",") + `"}`))
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
  contentFilenameBase := baseContentFilename[0:len(baseContentFilename) - len(extension)]

  query = fmt.Sprintf("SELECT presetId,CONCAT('%s/origin/vod/%s/%s_',`name`),presetIdDependance,doAnalyze FROM presets AS pr WHERE pr.profileId=? ORDER BY pr.presetIdDependance", outputBasePath, contentFilenameBase, uuid)
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
  presetToAssetIdMap := make(map[string]*int)
  var assetIds []int64
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
    assetIdDependance = nil
    if presetIdDependance != nil {
      presetIdsDependance := strings.Split(*presetIdDependance, `,`)
      for _, p := range presetIdsDependance {
        if presetToAssetIdMap[p] != nil {
          *assetIdDependance += strconv.Itoa(*presetToAssetIdMap[p]) + `,`
        }
      }
      *assetIdDependance = (*assetIdDependance)[:len(*assetIdDependance)-1]
    }
    query = "INSERT INTO assets (`contentId`,`presetId`,`assetIdDependance`,`filename`,`doAnalyze`,`createdAt`) VALUES (?,?,?,?,?,NULL)"
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
    if assetIdDependance == nil {
      result, err = stmt.Exec(contentId, presetId, nil, outputFilename, doAnalyze)
    } else {
      result, err = stmt.Exec(contentId, presetId, *assetIdDependance, outputFilename, doAnalyze)
    }
    if err != nil {
      errStr := fmt.Sprintf("XX Error during query execution %s with %s: %s", query, m["profileId"].(float64), err)
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
    for k, v := range profilesParameters {
      query = "INSERT INTO profilesParameters (`profileId`,`assetId`,`parameter`,`value`,`createdAt`) VALUES (?,?,?,?,NOW())"
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
      _, err = stmt.Exec(m["profileId"].(float64), assetId, k, v)
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
  dbSetContentState(db, contentId, "scheduled")

  jsonAnswer := `{"assetsId":[`
  for _, a := range assetIds {
    log.Printf("assetId is: %#v", a)
    jsonAnswer += strconv.FormatInt(a, 10) + `,`
  }
  jsonAnswer = jsonAnswer[:len(jsonAnswer)-1] + `]}`

  w.Write([]byte(jsonAnswer))
}

func transcodePostHandler(w http.ResponseWriter, r *http.Request) {
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
  if m["profileId"] == nil  {
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

  db := openDb()
  defer db.Close()

  query := "INSERT INTO contentsProfiles (`contentId`, `profileId`) VALUES (?,?)"
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
  _, err = stmt.Exec(contentId, m["profileId"].(float64))
  if err != nil {
    errStr := fmt.Sprintf("XX Error during query execution %s with (%d,%f): %s", query, contentId, m["profileId"].(float64), err)
    log.Printf(errStr)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }

  transcode(w, r, m, contentId)
}

func assetsGetHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
  w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
  w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
  w.Header().Set("Access-Control-Allow-Origin", "*")
  params := mux.Vars(r)
  id := -1
  contentId := -1
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
  db := openDb()
  defer db.Close()

  // select a.* from assets as a left join contentsProfiles as cp on a.contentId=cp.contentId where a.contentId='10' and cp.profileId=2;
  // select * from assets where contentId='719' AND presetId IN (SELECT presetId FROM presets AS pr LEFT JOIN profiles AS p ON pr.profileId=p.profileId WHERE p.name='VIDEO0ENG_AUDIO0FRA_BOUYGUES' AND pr.type='ffmpeg');

  var query string
  if profileName != "" {
    id = contentId
    query = "SELECT * FROM assets WHERE contentId=? AND presetId IN (SELECT presetId FROM presets AS pr LEFT JOIN profiles AS p ON pr.profileId=p.profileId WHERE p.name=?"
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
  var a Assets
  var rows *sql.Rows
  if profileName != "" {
    if presetsType != "" {
      rows, err = stmt.Query(id, profileName, presetsType)
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
    err = rows.Scan(&a.AssetId, &a.ContentId, &a.PresetId, &a.AssetIdDependance, &a.Filename, &a.DoAnalyze, &a.State, &a.CreatedAt, &a.UpdatedAt)
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
    jsonAnswer = "[" + jsonAnswer[:len(jsonAnswer) - 1] + "]"
  } else {
    jsonAnswer = "[ ]"
  }
  w.Write([]byte(jsonAnswer))
}

func encodersGetHandler(w http.ResponseWriter, r *http.Request) {
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
  db := openDb()
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
  var fp FFmpegProgress
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
  jsonAnswer = jsonAnswer[:len(jsonAnswer) - 1]
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer + "]"
  }
  w.Write([]byte(jsonAnswer))
}

func presetsGetHandler(w http.ResponseWriter, r *http.Request) {
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
    err = rows.Scan(&p.PresetId, &p.ProfileId, &p.PresetIdDependance, &p.Name, &p.Type, &p.DoAnalyze, &p.CmdLine, &p.CreatedAt, &p.UpdatedAt)
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

func contentsStreamsGetHandler(w http.ResponseWriter, r *http.Request) {
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

  db := openDb()
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
  var s ContentsStreams
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
    err = rows.Scan(&s.ContentsStreamId, &s.ContentId, &s.MapId, &s.Type, &s.Language, &s.Codec, &s.CodecInfo, &s.CodecProfile, &s.Bitrate, &s.Frequency, &s.Width, &s.Height, &s.Fps, &s.CreatedAt, &s.UpdatedAt)
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
    jsonAnswer = "[" + jsonAnswer[:len(jsonAnswer) - 1] + "]"
  } else {
    jsonAnswer = "[ ]"
  }
  w.Write([]byte(jsonAnswer))
}

func contentsStreamsPostHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
  w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
  w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
  w.Header().Set("Access-Control-Allow-Origin", "*")

  db := openDb()
  defer db.Close()

  var query string
  query = "SELECT contentId,filename FROM contents"
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
  rows, err = stmt.Query()
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
  var contentId int
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
      continue;
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

func contentsStreamsPutHandler(w http.ResponseWriter, r *http.Request) {
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

  db := openDb()
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

func assetsStreamsGetHandler(w http.ResponseWriter, r *http.Request) {
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

  db := openDb()
  defer db.Close()

  var query string
  if md5Hash != "" {
    query = "SELECT ass.* FROM assets AS a RIGHT JOIN assetsStreams AS ass ON a.assetId=ass.assetId WHERE contentId=(select contentId from contents where md5Hash=?)"
    if profileName != "" {
      query += " AND presetId IN (SELECT presetId FROM presets AS pr LEFT JOIN profiles AS p ON pr.profileId=p.profileId WHERE p.name=?);"
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
  var s AssetsStreams
  var rows *sql.Rows
  if md5Hash != "" {
    if profileName != "" {
      rows, err = stmt.Query(md5Hash, profileName)
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
    jsonAnswer = "[" + jsonAnswer[:len(jsonAnswer) - 1] + "]"
  } else {
    jsonAnswer = "[ ]"
  }
  w.Write([]byte(jsonAnswer))
}

func assetsStreamsPostHandler(w http.ResponseWriter, r *http.Request) {
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

  db := openDb()
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
    rows,err = stmt.Query(assetId)
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
      continue;
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

func contentsMd5PostHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  w.Header().Set("Content-Type", "application/json")
  w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
  w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
  w.Header().Set("Access-Control-Allow-Origin", "*")

  db := openDb()
  defer db.Close()

  query := "SELECT contentId, filename FROM contents WHERE md5Hash = ''"
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

func packagePostHandler(w http.ResponseWriter, r *http.Request) {
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

func profilesParametersGetHandler(w http.ResponseWriter, r *http.Request) {
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

  db := openDb()
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
  var pp ProfilesParameters
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
    err = rows.Scan(&pp.ProfileParameterId, &pp.ProfileId, &pp.Parameter, &pp.Value, &pp.CreatedAt, &pp.UpdatedAt)
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
    jsonAnswer = jsonAnswer[:len(jsonAnswer) - 1]
  }
  if rowsNumber > 1 {
    jsonAnswer = "[" + jsonAnswer + "]"
  }
  w.Write([]byte(jsonAnswer))
}

func setSubtitlesPostHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  body, _ := ioutil.ReadAll(r.Body)
  w.Header().Set("Content-Type", "application/json")
  w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
  w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
  w.Header().Set("Access-Control-Allow-Origin", "*")
  var jss JsonSetSubtitles
  err = json.Unmarshal(body, &jss)
  if err != nil {
    errStr := fmt.Sprintf("XX Cannot decode JSON %s: %s", body, err)
    log.Printf(errStr)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  params := mux.Vars(r)
  contentId := -1
  if params["uuid"] != "" {
    contentId, err = uuidToContentId(params["uuid"])
    if err != nil {
      errStr := fmt.Sprintf("XX Cannot get contentId from uuid %s: %s", params["uuid"], err)
      log.Printf(errStr)
      jsonStr := `{"error":"` + err.Error() + `"}`
      w.WriteHeader(http.StatusNotFound)
      w.Write([]byte(jsonStr))
      return
    }
  }

  db := openDb()
  defer db.Close()
  for p, s := range jss.S {
    query := "SELECT name FROM contents AS c LEFT JOIN profiles AS pr ON pr.profileId=c.profileId WHERE c.profileId=? AND c.contentId=?"
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
    var profileName string
    err = stmt.QueryRow(s.ProfileId, contentId).Scan(&profileName)
    if err != nil {
      errStr := fmt.Sprintf("XX Cannot Scan result query %s with (%d, %d): %s", query, s.ProfileId, contentId, err)
      log.Printf(errStr)
      jsonStr := `{"error":"` + err.Error() + `"}`
      w.WriteHeader(http.StatusNotFound)
      w.Write([]byte(jsonStr))
      return
    }
    var profileType string
    stringSearch := fmt.Sprintf("_SUB%d%s", p, strings.ToUpper(s.Language))
    if strings.Contains(profileName, stringSearch) == false {
      jsonStr := `{"error":"this profile doesn't accept subtitles ` + s.Language + `"}`
      w.WriteHeader(http.StatusNotFound)
      w.Write([]byte(jsonStr))
      return
    }
    if strings.Contains(profileName, "BOUYGUES") {
      profileType = "bouygues"
    }
    if strings.Contains(profileName, "USP") {
      profileType = "usp"
    }

    log.Printf("-- Set subtitles for profile type %s", profileType)
    switch (profileType) {
      case "bouygues":
        m := map[string]interface{}{}
        m["url"] = s.Url
        m["profileId"] = fmt.Sprintf("%d", s.ProfileId)
        transcode(w, r, m, contentId)
      case "usp":
    }
  }
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
    log.Printf("cu is %#v", cu)
    log.Printf("filename to package: %#v", filenames)
    var uspPackageArgs []string
    uspPackageArgs = append(uspPackageArgs, path.Dir(filenames[0]))
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
  ticker := time.NewTicker(time.Second * 1)
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
          publishExchange(ch, queueName, body)
        }
      }

      for contentId, _ := range contentIdsMap {
        dbSetContentState(db, contentId, "processing")
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
      go packageContents(contentUuids)

      db.Close()
    }
  }()
}

func optionsGetHandler(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
  w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
  w.Header().Set("Access-Control-Allow-Origin", "*")
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
  r.HandleFunc("/{a:.*}", optionsGetHandler).Methods("OPTIONS")
  r.HandleFunc("/api/contents", contentsGetHandler).Methods("GET").Queries("state", "{state:(initialized|scheduled|processing|failed|ready)}", "uuid", "{uuid:[0-9a-fA-F\\-]+}")
  r.HandleFunc("/api/contents", contentsGetHandler).Methods("GET").Queries("uuid", "{uuid:[0-9a-fA-F\\-]+}")
  r.HandleFunc("/api/contents", contentsGetHandler).Methods("GET").Queries("state", "{state:(initialized|scheduled|processing|failed|ready)}")
  r.HandleFunc("/api/contents", contentsGetHandler).Methods("GET").Queries("md5Hash", "{md5Hash:[0-9a-f\\-]+}")
  r.HandleFunc("/api/contents", contentsGetHandler).Methods("GET")
  r.HandleFunc("/api/contents", contentsPostHandler).Methods("POST")
  r.HandleFunc("/api/contents/{id:[0-9]+}", contentsGetHandler).Methods("GET")
  r.HandleFunc("/api/contents/{id:[0-9a-z\\-]*}/contentsStreams", contentsStreamsGetHandler).Methods("GET")
  r.HandleFunc("/api/contents/{contentId:[0-9a-z\\-]*}/assetsStreams", assetsStreamsGetHandler).Methods("GET")
  r.HandleFunc("/api/contents/{contentId:[0-9]+}/assets", assetsGetHandler).Methods("GET").Queries("profileName", "{profileName:.*}", "presetsType", "{presetsType:.*}")
  r.HandleFunc("/api/contents/{contentId:[0-9]+}/assets", assetsGetHandler).Methods("GET").Queries("profileName", "{profileName:.*}")
  r.HandleFunc("/api/contents/{contentId:[0-9]+}/assets", assetsGetHandler).Methods("GET")
  r.HandleFunc("/api/contents/{contentId:[0-9]+}/profiles/{profileId:[0-9]+}/assets", assetsGetHandler).Methods("GET")
  r.HandleFunc("/api/assets", assetsGetHandler).Methods("GET")
  r.HandleFunc("/api/assets/{id:[0-9]+}", assetsGetHandler).Methods("GET")
  r.HandleFunc("/api/assets/{id:[0-9]+}/ffmpegLogs/current", ffmpegLogsGetHandler).Methods("GET")
  r.HandleFunc("/api/assets/{id:[0-9]+}/ffmpegProgress/current", ffmpegProgressGetHandler).Methods("GET")
  r.HandleFunc("/api/assets/{assetId:[0-9]+}/assetsStreams", assetsStreamsPostHandler).Methods("POST")
  r.HandleFunc("/api/encoders", encodersGetHandler).Methods("GET")
  r.HandleFunc("/api/encoders/{id:[0-9]+}", encodersGetHandler).Methods("GET")
  r.HandleFunc("/api/logs", ffmpegLogsGetHandler).Methods("GET")
  r.HandleFunc("/api/logs/{assetId:[0-9]+}", ffmpegLogsGetHandler).Methods("GET")
  r.HandleFunc("/api/ffmpegProgress", ffmpegProgressGetHandler).Methods("GET").Queries("assetId", "{assetId:[0-9]+}")
  r.HandleFunc("/api/ffmpegProgress", ffmpegProgressGetHandler).Methods("GET")
  r.HandleFunc("/api/presets", presetsGetHandler).Methods("GET")
  r.HandleFunc("/api/presets/{id:[0-9]+}", presetsGetHandler).Methods("GET")
  r.HandleFunc("/api/profiles", profilesGetHandler).Methods("GET")
  r.HandleFunc("/api/profiles/{id:[0-9]+}", profilesGetHandler).Methods("GET")
  r.HandleFunc("/api/profiles/{profileId:[0-9]+}/contents", contentsGetHandler).Methods("GET")
  r.HandleFunc("/api/profiles/{profileId:[0-9]+}/presets", presetsGetHandler).Methods("GET")
  r.HandleFunc("/api/contentsStreams", contentsStreamsGetHandler).Methods("GET")
  r.HandleFunc("/api/contentsStreams", contentsStreamsPostHandler).Methods("POST")
  r.HandleFunc("/api/contentsStreams/{contentsStreamId:[0-9]+}", contentsStreamsPutHandler).Methods("PUT")
  r.HandleFunc("/api/assetsStreams", assetsStreamsGetHandler).Methods("GET").Queries("md5Hash", "{md5Hash:[0-9a-f\\)]+}", "profileName", "{profileName:.*}")
  r.HandleFunc("/api/assetsStreams", assetsStreamsGetHandler).Methods("GET").Queries("md5Hash", "{md5Hash:[0-9a-f\\)]+}")
  r.HandleFunc("/api/assetsStreams", assetsStreamsGetHandler).Methods("GET")
  r.HandleFunc("/api/assetsStreams", assetsStreamsPostHandler).Methods("POST")
  r.HandleFunc("/api/contentsMd5", contentsMd5PostHandler).Methods("POST")
  r.HandleFunc("/api/assetsStreams/{assetId:[0-9]+}", assetsStreamsPostHandler).Methods("POST")
  r.HandleFunc("/api/profilesParameters", profilesParametersGetHandler).Methods("GET")
  r.HandleFunc("/api/package", packagePostHandler).Methods("POST")
  r.HandleFunc("/api/transcode", transcodePostHandler).Methods("POST")
  //r.HandleFunc("/api/transcode/{uuid:[0-9a-f\\-]*}", transcodePostHandler).Methods("POST")
  r.HandleFunc("/api/setSubtitles/{uuid:[0-9a-f\\-]*}", setSubtitlesPostHandler).Methods("POST")

  http.Handle("/", r)
  //http.ListenAndServe(":4000", handlers.CORS()(r))
  http.ListenAndServe(":4000", nil)

  body := `{ "hostname": "p-afsmenc-001", "assetId": 1 }`
  publishExchange(ch, q.Name, body)

  log.Printf(" [*] Sending message")
}
