#!/bin/sh

export GOPATH=/go/src/pfscheduler
export GOOS=linux
export GOARCH=amd64
export GOBIN=/go/app

cd /go/src/pfscheduler
go get -v
