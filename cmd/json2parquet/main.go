package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/thisissc/logevent"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

var (
	targetBucketName string
	targetPrefix string

func HandleRequest(ctx context.Context, s3Event events.S3Event) {
	for _, record := range s3Event.Records {
		s3Entity := record.S3
		s3Bucket := s3Entity.Bucket
		s3Object := s3Entity.Object
		ProcessS3File(s3Bucket.Name, s3Object.Key)
	}
}

func ProcessS3File(bucket, key string) {
	targetBucketName = os.Getenv("TARGET_BUCKET")
	if len(targetBucketName) == 0 {
		log.Println("ENV 'TARGET_BUCKET' is empty")
		return
	}

	targetPrefix = os.Getenv("TARGET_PREFIX")
	if len(targetPrefix) == 0 {
		log.Println("ENV 'TARGET_BUCKET' is empty")
		return
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Println(err)
		return
	}

	client := s3.NewFromConfig(cfg)

	// XXX: lambda /tmp directory storage lese than 512MB
	resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Println(err)
		return
	}

	reader, isGzip := detectGzip(resp.Body)

	pwMap := make(map[string]*writer.ParquetWriter, 0)
	bufMap := make(map[string]io.Reader, 0)

	var scanner *bufio.Scanner
	if isGzip {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			scanner = bufio.NewScanner(reader)
		} else {
			scanner = bufio.NewScanner(gzipReader)
		}
	} else {
		scanner = bufio.NewScanner(reader)
	}

	for scanner.Scan() {
		var obj logevent.LogEvent
		err := json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			continue
		}

		newKey := newObjectKey(key, int64(obj.CreateTime)) // 根据LogEvent.CreateTime确定S3 Object Key
		pw, ok := pwMap[newKey]
		if !ok {
			bufMap[newKey], pwMap[newKey] = newBufferParquetWriter()
			pw = pwMap[newKey]
		}

		if err = pw.Write(obj); err != nil {
			log.Println(newKey, err)
		}
	}

	uploader := manager.NewUploader(client)

	for key, pw := range pwMap {
		if err = pw.WriteStop(); err != nil {
			log.Println(key, err)
			continue
		}

		buf := bufMap[key]
		_, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(targetBucketName),
			Key:    aws.String(key),
			Body:   buf,
		})
		if err != nil {
			log.Println(err)
		}
	}
}

func newBufferParquetWriter() (io.Reader, *writer.ParquetWriter) {
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)

	pw, err := writer.NewParquetWriter(fw, &logevent.LogEvent{}, 4)
	if err != nil {
		log.Println(err)
		return nil, nil
	}
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	return &buf, pw
}

func newObjectKey(key string, createTime int64) string {
	filename := filepath.Base(key)

	// UTC to PRC
	loc, _ := time.LoadLocation("PRC")
	dt := time.Unix(createTime, 0).In(loc)
	dstFold := dt.Format("year=2006/month=01/day=02")

	return fmt.Sprintf("%s/%s/%s.parquet", targetPrefix, dstFold, filename)
}

func detectGzip(input io.Reader) (io.Reader, bool) {
	var headerBuf bytes.Buffer

	teer := io.TeeReader(input, &headerBuf)

	// You can detect that a file is gziped by checking if the first 2 bytes are equal to 0x1f8b
	testBytes := make([]byte, 2)
	teer.Read(testBytes) //read 2 bytes
	isGzip := testBytes[0] == 31 && testBytes[1] == 139

	newr := io.MultiReader(&headerBuf, input)

	return newr, isGzip
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	lambda.Start(HandleRequest)
	//ProcessS3File("log.maijitv.com", "Firehose/logevent_firehose/2021/08/19/11/logevent_firehose-3-2021-08-19-11-00-59-dba60290-d1b5-47e6-8758-032a9ee676d4.gz")
}
