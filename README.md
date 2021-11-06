[![Go Report Card](https://goreportcard.com/badge/github.com/indiependente/s3stream)](https://goreportcard.com/report/github.com/indiependente/s3stream)
[![GoDoc](https://godoc.org/github.com/indiependente/s3stream?status.svg)](https://godoc.org/github.com/indiependente/s3stream)

# s3stream V3

## What is it?

**Streaming client for Amazon AWS S3**

Golang library that allows Get and Put operations from/to Amazon S3 in streaming fashion.

## V3

V3 of this package is compatible with <https://github.com/aws/aws-sdk-go-v2>

## How to install

```shell
go get github.com/indiependente/s3stream/v3
```

## Usage

Get:

```go
rc, err := store.Get(ctx, prefix, bucketname, filename)
```

`rc` is an `io.ReadCloser` which you can stream from.

Put:

```go
n, err := store.Put(ctx, prefix, bucketname, filename, r)
```

`r` is an `io.Reader` and Put will stream its content to the specified file in the desired bucket.

Look at the tests for more info on its usage.

You can run the tests locally by using Minio (<https://github.com/minio/minio>).
Example:

```shell
docker compose up  
go test -race -cover ./...
docker compose down
```
