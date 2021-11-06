all: test

.PHONY: deps-init
deps-init:
	rm go.mod go.sum
	go mod init
	go mod tidy

.PHONY: deps
deps:
	go mod download

.PHONY: lint
lint:
	golangci-lint run


.PHONY: test
test:
	docker compose up -d
	go test -race -cover ./...
	docker compose down
