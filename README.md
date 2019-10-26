# Fault-Tolerant Sharded Key-Value Store
> Simple key-value store system that tolerant crash fault through replication.

This system used Docker to create an image. The distributed key-value store system implemneted the REST API.
Using partitioning by hash to assign replicas with different shard id and to store key-value pairs in every replicas within the same shard id.

## Installation

To create the subnet mynet with IP range 10.10.0.0/16, execute:
```sh
docker network create --subnet=10.10.0.0/16 mynet
```

Execute the following command to build your Docker image:
```sh
docker build -t assignment4-image .
```

To run the replicas, execute:
```sh
docker run -p 8082:8080 --net=mynet --ip=10.10.0.2 --name="node1" -e SOCKET_ADDRESS="10.10.0.2:8080" -e VIEW="10.10.0.2:8080 10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080" -e SHARD_COUNT="2" assignment4-image
```