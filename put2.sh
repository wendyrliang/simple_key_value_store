curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"value": "value2", "causal-metadata": "key1?10.10.0.3:8080:0"}' http://localhost:8082/key-value-store/key2