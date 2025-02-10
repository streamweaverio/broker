package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	S3 "github.com/aws/aws-sdk-go/service/s3"
)

// S3Client defines common S3 operations
type Client interface {
	GetObject(input *S3.GetObjectInput) (*S3.GetObjectOutput, error)
}

type S3ClientOptions struct {
	AccessKeyId     string
	AccessKeySecret string
	Region          string
}

func NewClient(opts *S3ClientOptions) (Client, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:      aws.String(opts.Region),
			Credentials: credentials.NewStaticCredentials(opts.AccessKeyId, opts.AccessKeySecret, ""),
		},
	})

	if err != nil {
		return nil, err
	}

	client := S3.New(sess)

	return client, nil
}
