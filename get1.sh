curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8082/key-value-store-shard/node-shard-id

curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8083/key-value-store-shard/node-shard-id

curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8084/key-value-store-shard/node-shard-id

curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8085/key-value-store-shard/shard-id-members/1

curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"value": "value1", "causal-metadata": ""}' http://localhost:8082/key-value-store/key1

curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"value": "value2", "causal-metadata": ""}' http://localhost:8082/key-value-store/key2

curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data "{'socket-address': '10.0.0.5:8080'}" http://localhost:8085/key-value-store-shard/add-member/1

