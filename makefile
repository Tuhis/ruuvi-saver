.PHONY: build run dev

build:
	go build -o ruuvi-saver cmd/ruuvi-saver/main.go

run:
	./ruuvi-saver

dev:
	reflex -r '\.go$$' -s -- sh -c 'go run cmd/ruuvi-saver/main.go'

test:
	go test -v ./...

test-cover:
	go test -cover ./...
