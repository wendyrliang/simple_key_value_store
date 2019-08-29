#!/bin/sh 
docker stop node2
docker rm node2
docker run -p 8083:8080 --net=my-net --ip=10.10.0.3 --name="node2" -e SOCKET_ADDRESS="10.10.0.3:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080" -e SHARD_COUNT="2" kvs-img