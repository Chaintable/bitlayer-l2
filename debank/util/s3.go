package util

import (
	"bytes"
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func NewS3Client(region string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	cfg.Region = region
	client := s3.NewFromConfig(cfg)
	return client, nil
}

func UploadFileToS3(uploader *s3.Client, bucket string, key string, data []byte, overWrite bool) error {
	input := &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	}

	_, err := uploader.PutObject(context.TODO(), input)
	if err != nil {
		if !overWrite {
			// If file already exists and we're not overwriting, return nil
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
				return nil
			}
		}
	}
	return err
}

func DownloadFileFromS3(downloader *s3.Client, bucket string, key string) ([]byte, error) {
	output, err := downloader.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer output.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(output.Body)
	return buf.Bytes(), nil
}

func DownloadFileFromS3Json(downloader *s3.Client, bucket string, key string, target any) error {
	b, err := DownloadFileFromS3(downloader, bucket, key)
	if err != nil {
		return err
	}
	return DecodeFromGzipJson(b, target)
}

func DownloadFileFromS3Rlp(downloader *s3.Client, bucket string, key string, target any) error {
	b, err := DownloadFileFromS3(downloader, bucket, key)
	if err != nil {
		return err
	}
	return DecodeFromRlp(b, target)
}
