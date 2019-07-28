docker run -p 8082:8080 --net=mynet --ip=10.10.0.2 --name="node1" -e SOCKET_ADDRESS="10.10.0.2:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080" -e SHARD_COUNT="2" myapp