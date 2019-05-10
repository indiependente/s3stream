all: test

.PHONY: deps-init
deps-init:
	rm go.mod go.sum
	@GO111MODULE=on go mod init
	@GO111MODULE=on go mod tidy

.PHONY: deps
deps:
	@GO111MODULE=on go mod download

.PHONY: test
test:
	@docker run --rm --name minio -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" -p 9000:9000 \
  -d minio/minio server /data ; \
	go test -race -cover ./...
	@docker stop minio
