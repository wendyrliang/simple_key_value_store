curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8082/key-value-store-shard/node-shard-id

curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8083/key-value-store-shard/node-shard-id

curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8084/key-value-store-shard/node-shard-id

curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8085/key-value-store-shard/shard-id-members/0

curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8082/key-value-store/key1