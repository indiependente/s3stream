[![Go Report Card](https://goreportcard.com/badge/github.com/indiependente/s3stream)](https://goreportcard.com/report/github.com/indiependente/s3stream)
# s3stream
**Streaming client for Amazon AWS S3**

Golang library that allows Get and Put operations from/to Amazon S3 in streaming fashion.

Get:
```go
rc, err := store.Get(prefix, bucketname, filename)
```

`rc` is an `io.ReadCloser` which you can stream from.

Put:

```go
n, err := store.Put(prefix, bucketname, filename, contentType, r)
```

`r` is an `io.Reader` and Put will stream its content to the specified file in the desired bucket.

Look at the tests for more info on its usage.

You can run the tests locally by using Minio (https://github.com/minio/minio).
Example:
```shell
docker run --rm --name minio -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" -p 9000:9000 \
  -d minio/minio server /data
  
go test ./...
```
