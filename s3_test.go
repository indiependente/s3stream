package s3stream_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/indiependente/s3stream/v2"
)

func TestStore_Get(t *testing.T) {
	type args struct {
		prefix     string
		bucketname string
		filename   string
	}

	conf := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", ""),
		Endpoint:         aws.String("http://localhost:9000"),
		Region:           aws.String("us-west-2"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	svc := s3.New(session.Must(session.NewSession(conf)))

	tests := []struct {
		name           string
		testdata       string
		args           args
		conf           *aws.Config
		r              io.ReadCloser
		expectedSha256 string
		wantErr        bool
	}{
		{
			name:     "test streaming - 2MB",
			testdata: "data/Photo by NASA (yZygONrUBe8).jpg",
			args: args{
				prefix:     "",
				bucketname: "tiny",
				filename:   "Photo by NASA (yZygONrUBe8).jpg",
			},
			conf:           conf,
			expectedSha256: "fa63b10d8592bf468203c554419cb07a281790c45265fdd8761e171ee77e5dae",
			wantErr:        false,
		},
		{
			name:     "test streaming - 16MB",
			testdata: "data/RC_2006-05.json",
			args: args{
				prefix:     "",
				bucketname: "small",
				filename:   "RC_2006-05.json",
			},
			conf:           conf,
			expectedSha256: "b0fa845e08d9228a8109018f70676744e36c3e02834b4849bc2721310111a656",
			wantErr:        false,
		},
		{
			name:     "test streaming - 45MB",
			testdata: "data/reviews.json.gz",
			args: args{
				prefix:     "",
				bucketname: "big",
				filename:   "reviews.json.gz",
			},
			conf:           conf,
			expectedSha256: "b87037787e9d5374ce72995a29ac192cde1573513c31abfd4ca7781d802b1e22",
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := s3stream.NewStoreWithClient(svc)
			err := initBucket(svc, tt.args.bucketname)
			if err != nil {
				t.Errorf("Could not create bucket, error = %v", err)
				return
			}
			defer teardown(t, svc, tt.args.bucketname, tt.args.filename)

			listBuckets(svc)

			tt.r = fileReader(t, tt.testdata)
			defer tt.r.Close()
			_, err = store.Put(tt.args.prefix, tt.args.bucketname, tt.args.filename, tt.r)
			if err != nil {
				t.Errorf("Store.Put() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			rc, err := store.Get(tt.args.prefix, tt.args.bucketname, tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("Store.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !checksum(t, rc, tt.expectedSha256) {
				t.Error("Put / Get failed, checksum mismatch")
				return
			}
		})
	}
}

func initBucket(svc *s3.S3, bucketname string) error {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketname),
	}

	result, err := svc.CreateBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				fmt.Println(s3.ErrCodeBucketAlreadyExists, aerr.Error())
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				fmt.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return err
	}
	fmt.Println(result)

	return nil
}

func listBuckets(svc *s3.S3) {
	input := &s3.ListBucketsInput{}

	result, err := svc.ListBuckets(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	fmt.Println(result)
}

func teardown(t *testing.T, svc *s3.S3, bucketname, filename string) {
	err := deleteFile(svc, bucketname, filename)
	if err != nil {
		t.Errorf("Could not delete file, error = %v", err)
		return
	}
	err = destroyBucket(svc, bucketname)
	if err != nil {
		t.Errorf("Could not delete bucket, error = %v", err)
		return
	}
}

func deleteFile(svc *s3.S3, bucketname, filename string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(filename),
	}

	result, err := svc.DeleteObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return err
	}

	fmt.Println(result)
	return nil
}

func destroyBucket(svc *s3.S3, bucketname string) error {
	input := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketname),
	}

	result, err := svc.DeleteBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return err
	}

	fmt.Println(result)
	return nil
}

func fileReader(t *testing.T, filename string) io.ReadCloser {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Errorf("%s does not exist", filename)
	}
	f, err := os.Open(filename)
	if err != nil {
		t.Errorf("Could not read input file, error = %v", err)
	}
	return f
}

func checksum(t *testing.T, r io.Reader, expectedSha256 string) bool {
	data, err := io.ReadAll(r)
	if err != nil {
		t.Errorf("Could not calculate checksum, error = %v", err)
		return false
	}
	shahex := sha256.Sum256(data)
	t.Logf("Expected = %s\nCalculated = %s\n", expectedSha256, hex.EncodeToString(shahex[:]))

	return hex.EncodeToString(shahex[:]) == expectedSha256
}
