package s3stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	readBlockSize    = 16 * 1024 * 1024       // 16 MB
	writeBlockSize   = 8 * 1024 * 1024        // 8 MB
	tempBlockSize    = 1 * 1024 * 1024        // 1 MB
	awsMaxParts      = 10000                  // https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
	awsMaxPartSize   = 5 * 1024 * 1024 * 1024 // 5 GB
	maxUploadRetries = 5
)

// Store is the S3 implementation of the Store interface.
type Store struct {
	api          *s3.Client
	readPartSize int64
}

// NewStore returns a Store given the input options.
func NewStore(conf aws.Config, opts ...func(*Store) error) (Store, error) {
	api := s3.NewFromConfig(conf)
	return NewStoreWithClient(api, opts...)
}

// NewStoreWithClient returns a Store given the input client.
func NewStoreWithClient(client *s3.Client, opts ...func(*Store) error) (Store, error) {
	s := Store{
		api:          client,
		readPartSize: readBlockSize,
	}
	for _, o := range opts {
		err := o(&s)
		if err != nil {
			return Store{}, err
		}
	}
	return s, nil
}

// WithReadPartSize allows to set the part size of the multipart get operation.
// The part size cannot be greater than the AWS MaxPartSize constant of 5GB.
// https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
func WithReadPartSize(size int64) func(s *Store) error {
	return func(s *Store) error {
		if size > awsMaxPartSize {
			return errors.New("part size is over AWS limits")
		}
		s.readPartSize = size
		return nil
	}
}

// Get returns the content of the file in input reading it from the underlying S3 bucket.
func (s Store) Get(ctx context.Context, prefix, bucketname, filename string) (io.ReadCloser, error) {
	objOut, err := s.api.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(prefix + filename),
	})
	if err != nil {
		return nil, fmt.Errorf("could not get metadata for object %s: %w", filename, err)
	}

	length := objOut.ContentLength
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close() // nolint: errcheck, gosec

		var (
			i, start, remainder int64
			rangeSpecifier      string
		)

		for i = 0; i < length/s.readPartSize; i++ {
			start = i * s.readPartSize
			rangeSpecifier = fmt.Sprintf("bytes=%d-%d", start, start+s.readPartSize-1)
			data, err := s.getDataInRange(ctx, prefix, bucketname, filename, rangeSpecifier)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("could not get data: %w", err)) // nolint: errcheck, gosec
			}
			_, err = pw.Write(data)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("could not write data: %w", err)) // nolint: errcheck, gosec
			}

		}
		remainder = length % s.readPartSize
		if remainder > 0 {
			start = (length / s.readPartSize) * s.readPartSize
			rangeSpecifier = fmt.Sprintf("bytes=%d-%d", start, start+remainder-1)
			data, err := s.getDataInRange(ctx, prefix, bucketname, filename, rangeSpecifier)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("could not get data: %w", err)) // nolint: errcheck, gosec
			}
			_, err = pw.Write(data)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("could not get data: %w", err)) // nolint: errcheck, gosec
			}
		}
	}()

	return pr, nil
}

func (s Store) getDataInRange(ctx context.Context, prefix, bucketname, filename, rangeSpecifier string) ([]byte, error) {
	resp, err := s.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(prefix + filename),
		Range:  aws.String(rangeSpecifier),
	})
	if err != nil {
		return nil, fmt.Errorf("could not get object %s: %w", rangeSpecifier, err)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read from resp.Body: %w", err)
	}
	defer resp.Body.Close() // nolint: errcheck, gosec
	return data, nil
}

// Put stores the content of the reader in input with the specified name.
// Returns number of bytes written and an error if any.
func (s Store) Put(ctx context.Context, prefix, bucketname, filename string, r io.Reader) (int, error) {
	// initialize upload
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(prefix + filename),
	}
	resp, err := s.api.CreateMultipartUpload(ctx, input)
	if err != nil {
		return 0, fmt.Errorf("could not create multipart upload: %w.", err)
	}

	var (
		reachedEOF     bool
		completedParts []types.CompletedPart
	)

	// buffering up to writeBlockSize MB and then uploading the block
	dataidx, total := 0, 0
	temp := make([]byte, tempBlockSize)
	data := make([]byte, writeBlockSize)
	i := 1
	for i <= awsMaxParts {
		// read into temporary buffer
		n, err := r.Read(temp)
		if err != nil {
			if err != io.EOF {
				readerr := fmt.Errorf("could not read part %d: %w", i, err)
				aberr := s.abortMultipartUpload(ctx, resp)
				if aberr != nil {
					return total, fmt.Errorf("could not abort upload: %w", readerr)
				}
				return total, readerr
			}
			reachedEOF = true
		}

		// if can't buffer more, upload and reset data counter
		if dataidx+n > writeBlockSize {
			completedPart, err := s.uploadPart(ctx, resp, data[:dataidx], i)
			if err != nil {
				uplderr := fmt.Errorf("could not upload part %d: %w", i, err)
				aberr := s.abortMultipartUpload(ctx, resp)
				if aberr != nil {
					return total, fmt.Errorf("could not abort upload: %w", uplderr)
				}
				return total, fmt.Errorf("upload aborted: %w", uplderr)
			}
			completedParts = append(completedParts, *completedPart)
			i++
			dataidx = 0
		}

		// buffer from temporary to storage buffer, increment counters
		copy(data[dataidx:], temp[:n])
		dataidx += n
		total += n

		// upload remaining content
		if reachedEOF {
			completedPart, err := s.uploadPart(ctx, resp, data[:dataidx], i)
			if err != nil {
				uplderr := fmt.Errorf("could not upload part %d: %w", i, err)
				aberr := s.abortMultipartUpload(ctx, resp)
				if aberr != nil {
					return total, fmt.Errorf("could not abort upload: %w", uplderr)
				}
				return total, fmt.Errorf("upload aborted: %w", uplderr)
			}
			completedParts = append(completedParts, *completedPart)
			break
		}
	}

	// check for which reason it got out of the loop
	if i > awsMaxParts && !reachedEOF {
		maxparterr := fmt.Errorf("could not upload whole content... MaxPartsNumber limit reached. Aborting: %w...", err)
		aberr := s.abortMultipartUpload(ctx, resp)
		if aberr != nil {
			return total, fmt.Errorf("could not abort upload: %w", maxparterr)
		}
		return total, maxparterr
	}

	// finalize upload
	_, err = s.completeMultipartUpload(ctx, resp, completedParts)
	if err != nil {
		return total, fmt.Errorf("error while completing upload: %w", err)
	}

	return total, nil
}

// uploadPart uploads a single part and returns a CompletedPart object and an error if any.
// It will retry five times if not able to upload.
func (s Store) uploadPart(ctx context.Context, resp *s3.CreateMultipartUploadOutput, data []byte, partNumber int) (*types.CompletedPart, error) {
	var uploadResult *s3.UploadPartOutput

	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(data),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    int32(partNumber),
		UploadId:      resp.UploadId,
		ContentLength: int64(len(data)),
	}

	for tryNum := 1; tryNum <= maxUploadRetries; tryNum++ {
		var err error
		uploadResult, err = s.api.UploadPart(ctx, partInput)
		if err != nil {
			if tryNum == maxUploadRetries {
				var apiErr smithy.APIError
				if errors.As(err, &apiErr) {
					code := apiErr.ErrorCode()
					message := apiErr.ErrorMessage()
					return nil, fmt.Errorf("aws error %s while uploading: retried %d times.: %s", code, tryNum, message)
				}
				return nil, fmt.Errorf("error while uploading: retried %d times.: %w", tryNum, err)
			}
		} else {
			break
		}
	}

	return &types.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: int32(partNumber),
	}, nil
}

// abortMultipartUpload aborts the multipart upload process
func (s Store) abortMultipartUpload(ctx context.Context, resp *s3.CreateMultipartUploadOutput) error {
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := s.api.AbortMultipartUpload(ctx, abortInput)
	return fmt.Errorf("could not abort upload: %w", err)
}

// completeMultipartUpload completes the multipart upload process
func (s Store) completeMultipartUpload(ctx context.Context, resp *s3.CreateMultipartUploadOutput, completedParts []types.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return s.api.CompleteMultipartUpload(ctx, completeInput)
}
