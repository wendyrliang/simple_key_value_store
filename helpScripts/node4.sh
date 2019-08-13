#!/bin/sh 
docker stop node4
docker rm node4
docker run -p 8085:8080 --net=my-net --ip=10.10.0.5 --name="node4" -e SOCKET_ADDRESS="10.10.0.5:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080" myapp