#!/bin/bash
trap 'kill %1; kill %2' SIGINT

node() {
    cd $1
    redis-server ./redis.conf
}

node 7000 | tee /tmp/recl0.log | sed -e 's/^/[7000] /' &
node 7001 | tee /tmp/recl1.log | sed -e 's/^/[7001] /' &
node 7002 | tee /tmp/recl2.log | sed -e 's/^/[7002] /' &
node 7003 | tee /tmp/recl3.log | sed -e 's/^/[7003] /' &
node 7004 | tee /tmp/recl4.log | sed -e 's/^/[7004] /' &
node 7005 | tee /tmp/recl5.log | sed -e 's/^/[7005] /'