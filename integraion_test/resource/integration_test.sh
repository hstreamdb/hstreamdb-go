#!/bin/bash

set -xeuo pipefail

function cluster_up() {
  docker pull hstreamdb/hstream
  docker pull zookeeper
  docker-compose up -d

  timeout=60
  until docker exec resource-hstore-admin-server-1 hadmin server --host hserver1 --port 6580 status > /dev/null 2>&1
  do
    >&2 echo 'Waiting for servers...'
    sleep 1
    timeout=$((timeout - 1))
    [ "$timeout" -le 0 ] && echo 'Timeout!' && exit 1;
  done
  echo "===> set up cluster success"
}

function cluster_down() {
  docker-compose down
  echo "===> cluster down"
}

cluster_up
go clean -testcache && go test -v -gcflags=-l -race -timeout=5m ../
cluster_down