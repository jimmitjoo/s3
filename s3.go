package s3

import (
	"bytes"
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Read from s3 bucket
func Read(region, bucket, filename string) ([]byte, error) {

	// Initialize a session in that the SDK will use to load
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	if err != nil {
		log.Error(err)

		return []byte{}, err
	}

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)

	buff := &aws.WriteAtBuffer{}

	// Write the contents of S3 Object to the file
	_, err = downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		log.Error(err)

		return []byte{}, err
	}
	return buff.Bytes(), nil
}

// Store to s3 bucket
func Store(region, bucket, key string, body []byte) error {

	// Initialize a session in that the SDK will use to load
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region)},
	))

	// Create a new instance of the service's client with a Session.
	svc := s3.New(sess)

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()

	timeout := time.Duration(10) * time.Minute
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	defer cancelFn()

	// Uploads the object to S3. The Context will interrupt the request if the
	// timeout expires.
	_, err := svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Printf("upload canceled due to timeout")
		} else {
			fmt.Printf("Failed to upload object.\n Region: %v \n Bucket: %v \n", region, bucket)
		}
		return err
	}

	fields := log.Fields{
		"bucket": bucket,
		"key":    key,
	}

	log.WithField("params", fields).Info("Upload to S3 done")

	return nil
}

// MoveToDir creates a copy of a file and creates it with new key
func MoveToDir(oldKey string, newKey string, bucket string, sess *session.Session, svc *s3.S3) {

	// Create a context with a timeout that will abort the copy if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()

	timeout := time.Duration(10) * time.Minute
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}

	// Ensure the context is canceled to prevent leaking.
	defer cancelFn()

	_, err := svc.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(bucket + "/" + oldKey),
		Key:        aws.String(newKey),
	})

	if err != nil {
		fmt.Println("=== Could not copy file ===")
		fmt.Println("OldKey: 		", bucket+"/"+oldKey)
		fmt.Println("nnewKey: 	", newKey)
		fmt.Println("bucket: 		", bucket)
		fmt.Println("Error: 		", err)
		fmt.Println("===")
		return
	}

	_, err = svc.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(oldKey),
	})

	if err != nil {
		fmt.Printf("File %v moved to %v, but not deleted", oldKey, newKey)
	}
}
