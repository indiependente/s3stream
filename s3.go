package s3stream

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pkg/errors"
)

const (
	readBlockSize    = 16 * 1024 * 1024 // 16 MB
	writeBlockSize   = 8 * 1024 * 1024  // 8 MB
	tempBlockSize    = 1 * 1024 * 1024  // 1 MB
	awsMaxParts      = 10000            // https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
	maxUploadRetries = 5
)

// Store is the S3 implementation of the Store interface.
type Store struct {
	api *s3.S3
}

// NewStore returns a Store given the input options.
func NewStore(conf *aws.Config) Store {
	sess := session.Must(session.NewSession(conf))
	api := s3.New(sess)

	return Store{
		api: api,
	}
}

// NewStoreWithClient returns a Store given the input client.
func NewStoreWithClient(client *s3.S3) Store {
	return Store{
		api: client,
	}
}

// Get returns the content of the file in input reading it from the underlying S3 bucket.
func (s Store) Get(prefix, bucketname, filename string) (io.ReadCloser, error) {
	objOut, err := s.api.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(prefix + filename),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get metadata for object %s", filename)
	}

	length := *objOut.ContentLength
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close() // nolint: errcheck, gosec

		var (
			i, start, remainder int64
			rangeSpecifier      string
		)

		for i = 0; i < length/readBlockSize; i++ {
			start = i * readBlockSize
			rangeSpecifier = fmt.Sprintf("bytes=%d-%d", start, start+readBlockSize-1)
			data, err := s.getDataInRange(prefix, bucketname, filename, rangeSpecifier)
			if err != nil {
				pw.CloseWithError(errors.Wrap(err, "Could not get data")) // nolint: errcheck, gosec
			}
			_, err = pw.Write(data)
			if err != nil {
				pw.CloseWithError(errors.Wrap(err, "Could not write data")) // nolint: errcheck, gosec
			}

		}
		remainder = length % readBlockSize
		if remainder > 0 {
			start = (length / readBlockSize) * readBlockSize
			rangeSpecifier = fmt.Sprintf("bytes=%d-%d", start, start+remainder-1)
			data, err := s.getDataInRange(prefix, bucketname, filename, rangeSpecifier)
			if err != nil {
				pw.CloseWithError(errors.Wrap(err, "Could not get data")) // nolint: errcheck, gosec
			}
			_, err = pw.Write(data)
			if err != nil {
				pw.CloseWithError(errors.Wrap(err, "Could not get data")) // nolint: errcheck, gosec
			}
		}
	}()

	return pr, nil
}

func (s Store) getDataInRange(prefix, bucketname, filename, rangeSpecifier string) ([]byte, error) {
	resp, err := s.api.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(prefix + filename),
		Range:  aws.String(rangeSpecifier),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get object %s", rangeSpecifier)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not read from resp.Body")
	}
	defer resp.Body.Close() // nolint: errcheck, gosec
	return data, nil
}

// Put stores the content of the reader in input with the specified name.
// Returns number of bytes written and an error if any.
func (s Store) Put(prefix, bucketname, filename string, contentType string, r io.Reader) (int, error) {
	// initialize upload
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucketname),
		Key:         aws.String(prefix + filename),
		ContentType: aws.String(contentType),
	}
	resp, err := s.api.CreateMultipartUpload(input)
	if err != nil {
		return 0, errors.Wrap(err, "Could not create multipart upload.")
	}

	var (
		reachedEOF     bool
		completedParts []*s3.CompletedPart
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
				readerr := errors.Wrapf(err, "Could not read part %d", i)
				aberr := s.abortMultipartUpload(resp)
				if aberr != nil {
					return total, errors.Wrap(readerr, "Could not abort upload")
				}
				return total, readerr
			}
			reachedEOF = true
		}

		// if can't buffer more, upload and reset data counter
		if dataidx+n > writeBlockSize {
			completedPart, err := s.uploadPart(resp, data[:dataidx], i)
			if err != nil {
				uplderr := errors.Wrapf(err, "Could not upload part %d", i)
				aberr := s.abortMultipartUpload(resp)
				if aberr != nil {
					return total, errors.Wrap(uplderr, "Could not abort upload")
				}
				return total, errors.Wrap(uplderr, "Upload aborted")
			}
			completedParts = append(completedParts, completedPart)
			i++
			dataidx = 0
		}

		// buffer from temporary to storage buffer, increment counters
		copy(data[dataidx:], temp[:n])
		dataidx += n
		total += n

		// upload remaining content
		if reachedEOF {
			completedPart, err := s.uploadPart(resp, data[:dataidx], i)
			if err != nil {
				uplderr := errors.Wrapf(err, "Could not upload part %d", i)
				aberr := s.abortMultipartUpload(resp)
				if aberr != nil {
					return total, errors.Wrap(uplderr, "Could not abort upload")
				}
				return total, errors.Wrap(uplderr, "Upload aborted")
			}
			completedParts = append(completedParts, completedPart)
			break
		}
	}

	// check for which reason it got out of the loop
	if i > awsMaxParts && !reachedEOF {
		maxparterr := errors.Wrap(err, "Could not upload whole content... MaxPartsNumber limit reached. Aborting...")
		aberr := s.abortMultipartUpload(resp)
		if aberr != nil {
			return total, errors.Wrap(maxparterr, "Could not abort upload")
		}
		return total, maxparterr
	}

	// finalize upload
	_, err = s.completeMultipartUpload(resp, completedParts)
	if err != nil {
		return total, errors.Wrap(err, "Error while completing upload")
	}

	return total, nil
}

// uploadPart uploads a single part and returns a CompletedPart object and an error if any.
// It will retry five times if not able to upload.
func (s Store) uploadPart(resp *s3.CreateMultipartUploadOutput, data []byte, partNumber int) (*s3.CompletedPart, error) {

	var (
		uploadResult *s3.UploadPartOutput
	)

	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(data),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(data))),
	}

	for tryNum := 1; tryNum <= maxUploadRetries; tryNum++ {
		var err error
		uploadResult, err = s.api.UploadPart(partInput)
		if err != nil {
			if tryNum == maxUploadRetries {
				if anerr, ok := err.(awserr.Error); ok {
					return nil, errors.Wrapf(anerr, "AWS Error while uploading: retried %d times.", tryNum)
				}
				return nil, errors.Wrapf(err, "Error while uploading: retried %d times.", tryNum)
			}
		} else {
			break
		}
	}

	return &s3.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: aws.Int64(int64(partNumber)),
	}, nil

}

// abortMultipartUpload aborts the multipart upload process
func (s Store) abortMultipartUpload(resp *s3.CreateMultipartUploadOutput) error {
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := s.api.AbortMultipartUpload(abortInput)
	return errors.Wrap(err, "Could not abort upload")
}

// completeMultipartUpload completes the multipart upload process
func (s Store) completeMultipartUpload(resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return s.api.CompleteMultipartUpload(completeInput)
}
