package main

import (
  "io"
  "os"
  "os/exec"
  "path"
  "regexp"
  "log"
  "fmt"
  "io/ioutil"
  "encoding/json"
  "net/http"
  "github.com/gorilla/mux"
)

type JsonTransfer struct {
  Url *string `json:"url"`
}

func bouyguesTransferPostHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  var out []byte
  out, err = exec.Command(`/usr/bin/ssh-keygen`, `-f`, os.Getenv("HOME") + `/.ssh/known_hosts`, `-R`, `195.36.151.236`).Output()
  if err != nil {
    log.Printf("XX Cannot execute cmd ssh-keygen for deleting key: %s: %s", out, err)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  var cmd *exec.Cmd
  cmd = exec.Command(`/usr/bin/sftp`, `-oStrictHostKeyChecking=no`, `-i`, `/home/ubuntu/bouygues-sftp-rsa`, `-b`, `-`, `videovasrep@195.36.151.236:/public`)

  transferPostHandler(w, r, cmd)
}

func orangeTransferPostHandler(w http.ResponseWriter, r *http.Request) {
  var cmd *exec.Cmd
  cmd = exec.Command(`/usr/bin/sftp`, `-oStrictHostKeyChecking=no`, `-i`, `/home/ubuntu/.ssh/id_rsa_bouygues`, `-b`, `-`, `afrostream_qualif@ftp.orangeportails.net:/upload`)

  transferPostHandler(w, r, cmd)
}

func mmdmPreprodTransferPostHandler(w http.ResponseWriter, r *http.Request) {
  var cmd *exec.Cmd
  cmd = exec.Command(`/usr/bin/sftp`, `-oStrictHostKeyChecking=no`, `-i`, `/home/ubuntu/.ssh/id_dsa_orange`, `-b`, `-`, `AFRMM1@transfert-pp.francetelecom.com:/IN/AFRMM101`)

  transferPostHandler(w, r, cmd)
}

func mmdmTransferPostHandler(w http.ResponseWriter, r *http.Request) {
  var cmd *exec.Cmd
  cmd = exec.Command(`/usr/bin/sftp`, `-oStrictHostKeyChecking=no`, `-i`, `/home/ubuntu/.ssh/id_dsa_orange`, `-b`, `-`, `AFRMMD@transfert.francetelecom.com:/IN/AFRMMD01`)

  transferPostHandler(w, r, cmd)
}

func transferPostHandler(w http.ResponseWriter, r *http.Request, cmd *exec.Cmd) {
  var err error
  body, _ := ioutil.ReadAll(r.Body)
  w.Header().Set("Content-Type", "application/json")
  w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Accept")
  w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
  w.Header().Set("Access-Control-Allow-Origin", "*")
  var jt JsonTransfer
  err = json.Unmarshal(body, &jt)
  if err != nil {
    errStr := fmt.Sprintf("XX Cannot decode JSON %s: %s", body, err)
    log.Printf(errStr)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  if jt.Url == nil {
    errStr := fmt.Sprintf("XX JSON parameter Url is missing")
    log.Printf(errStr)
    jsonStr := `{"error":"'url' parameter is missing"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }

  reUrl, err := regexp.Compile("https?:\\/\\/[^\\/]*\\/.*\\..*")
  if err != nil {
    return
  }
  matches := reUrl.FindAllStringSubmatch(*jt.Url, -1)
  if matches == nil {
    errStr := fmt.Sprintf("XX JSON parameter Url is incorrect")
    log.Printf(errStr)
    jsonStr := `{"error":"'url' parameter is incorrect"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }

  resp, err := http.Get(*jt.Url)
  if err != nil {
    errStr := fmt.Sprintf("XX Cannot get url '%s': %s", jt.Url, err)
    log.Printf(errStr)
    jsonStr := `{"error":"`+ err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  defer resp.Body.Close()
  if resp.StatusCode != 200 {
    errStr := fmt.Sprintf("Cannot get url %s, HTTP return status code is %d", jt.Url, err)
    log.Printf(errStr)
    jsonStr := `{"error":"` + errStr + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  body, err = ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Printf("XX Cannot ReadAll resp.Body '%#v': %s", body, err)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  var name string
  name, err = ioutil.TempDir(`/tmp`, `transfer`)
  if err != nil {
    errStr := fmt.Sprintf("XX Cannot create temp directory: %s", jt.Url, err)
    log.Printf(errStr)
    jsonStr := `{"error":"`+ err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  filename := name + `/` + path.Base(*jt.Url)
  log.Printf("-- Writing url to file '%s'", filename)
  err = ioutil.WriteFile(filename, body, 0644)
  if err != nil {
    log.Printf("XX Cannot open file '%s' for writing: %s", filename, err)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }

  var stdin io.WriteCloser
  stdin, err = cmd.StdinPipe()
  if err != nil {
    log.Printf("XX Cannot create stdin pipe: %s", err)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  err = cmd.Start()
  if err != nil {
    log.Printf("XX Cannot Start cmd sftp for transferring file '%s': %s", filename, err)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }
  log.Printf("run sftp with command: mput %s\nbye\n", filename)
  stdin.Write([]byte(`mput ` + filename + "\n"))
  stdin.Write([]byte(`bye` + "\n"))
  err = cmd.Wait()
  if err != nil {
    log.Printf("XX Cannot Wait cmd sftp for transferring file '%s': %s", filename, err)
    jsonStr := `{"error":"` + err.Error() + `"}`
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte(jsonStr))
    return
  }

  jsonStr := `{"result":"success"}`
  w.Write([]byte(jsonStr))
  return
}

func main() {
  r := mux.NewRouter()
  r.HandleFunc("/api/transfer", bouyguesTransferPostHandler).Methods("POST")
  r.HandleFunc("/api/orangeTransfer", orangeTransferPostHandler).Methods("POST")
  r.HandleFunc("/api/mmdmPreprodTransfer", mmdmPreprodTransferPostHandler).Methods("POST")
  r.HandleFunc("/api/mmdmTransfer", mmdmTransferPostHandler).Methods("POST")

  http.Handle("/", r)
  http.ListenAndServe(":4001", nil)
}
