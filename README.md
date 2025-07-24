[![Go Report Card](https://goreportcard.com/badge/github.com/indiependente/s3stream)](https://goreportcard.com/report/github.com/indiependente/s3stream)
[![GoDoc](https://godoc.org/github.com/indiependente/s3stream?status.svg)](https://pkg.go.dev/github.com/indiependente/s3stream/v3)

# s3stream (v3)

**A streaming interface for Amazon S3 using the AWS Go SDK v2.**

`s3stream` provides a clean and idiomatic API for streaming large files to and from S3 buckets without loading them entirely into memory. It uses multipart uploads for writing and byte-range reads for downloading, making it suitable for large files, low-memory environments, and high-performance applications.

---

## ✅ Features

- Stream reads from S3 with ranged `GetObject` requests.
- Stream writes to S3 using multipart uploads with retry and error handling.
- Customisable part sizes for reads.
- Context-aware operations with cancellation support.
- Compatible with [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2).
- Easily testable using MinIO.

---

## 🔧 Installation

```bash
go get github.com/indiependente/s3stream/v3
```

---

## 📦 API Usage

### Create a Store

```go
import "github.com/indiependente/s3stream/v3"

store, err := s3stream.NewStore(cfg) // cfg is aws.Config
if err != nil {
    log.Fatal(err)
}
```

Or use an existing S3 client:

```go
client := s3.NewFromConfig(cfg)
store, err := s3stream.NewStoreWithClient(client)
```

---

### Stream Upload (`Put`)

```go
f, _ := os.Open("largefile.bin")
defer f.Close()

n, err := store.Put(ctx, "prefix/", "my-bucket", "largefile.bin", f)
if err != nil {
    log.Fatalf("upload failed: %v", err)
}
fmt.Printf("Uploaded %d bytes\n", n)
```

- Uses multipart upload with 8 MB buffered chunks.
- Automatically retries failed parts and aborts on failure.

---

### Stream Download (`Get`)

```go
r, err := store.Get(ctx, "prefix/", "my-bucket", "largefile.bin")
if err != nil {
    log.Fatalf("download failed: %v", err)
}
defer r.Close()

io.Copy(os.Stdout, r)
```

- Download is performed using sequential ranged `GetObject` calls (default 16 MB per chunk).

---

### Custom Read Chunk Size (Optional)

```go
store, err := s3stream.NewStore(cfg, s3stream.WithReadPartSize(32 * 1024 * 1024)) // 32 MB
```

---

## 🧪 Testing with MinIO

You can run integration tests locally using Docker and MinIO:

```bash
docker compose up -d
go test -race -cover ./...
docker compose down
```

See `s3_test.go` for examples covering upload, download, and SHA256 checksum validation.

---

## 📚 Resources

- [AWS S3 Multipart Upload Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
- [AWS SDK for Go v2](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2)

---

## 🔒 Considerations

- Automatically aborts uploads on error.
- Enforces AWS multipart limits: 10,000 parts max, 5 GB per part.
- Defaults:
  - Write buffer: 8 MB
  - Read chunk size: 16 MB
  - Temp read buffer: 1 MB

---

## License

MIT

> Built and maintained by [@indiependente](https://github.com/indiependente)
