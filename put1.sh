curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"value": "value1", "causal-metadata": ""}' http://localhost:8082/key-value-store/key1
