
ENV=CGO_ENABLE=0 GOOS=linux GOARCH=amd64

all: cli svr

cli: client/* ../pkg/common/* ../pkg/client/*
	$(ENV) go build -o cli client/*

svr: server/* ../pkg/common/* ../pkg/basic_paxos/*
	$(ENV) go build -o svr server/main.go

test: svr
	kill $(shell pidof svr) || true
	# ./svr 0 :22222 :22223 :22224 &
	# ./svr 1 :22222 :22223 :22224 &
	# ./svr 2 :22222 :22223 :22224 &
