package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/truflation/tsn-db/internal/benchmark/benchexport"
)

// Parameters:
// - bucket: string
// - key: string

type Event struct {
	Bucket string `json:"bucket"`
	// key = <timestamp>_<instance_type>.csv
	Key string `json:"key"`
}

func HandleRequest(ctx context.Context, event Event) error {
	// get the results from the results bucket
	// all results are like this: s3://<bucket>/<timestamp>_<instance_type>.csv
	// <timestamp> is the report time in 2024-05-02T12:00:00.000Z
	// we want to get all the keys and then download them all into a tmp folder
	// then we merge them into a single file

	// get all the keys from the results bucket
	resp, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(event.Bucket),
		Prefix: aws.String(event.Key),
	})

	csvFiles := make([]string, 0)

	// get only csv files
	for _, obj := range resp.Contents {
		// matches /<key>_<instance_type>.csv
		if regexp.MustCompile(`^.*_.*\.csv$`).MatchString(*obj.Key) {
			csvFiles = append(csvFiles, *obj.Key)
		}
	}

	// sort csv files
	sort.Strings(csvFiles)

	// download all the csv files
	for _, csvFile := range csvFiles {
		reportTime, err := time.Parse("2006-01-02T15:04:05.000Z", csvFile[:19])
		// get instance type from the key
		instanceType := csvFile[strings.LastIndex(csvFile, "_")+1:]

		if err != nil {
			return err
		}

		// download the file
		resp, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(event.Bucket),
			Key:    aws.String(csvFile),
		})

		if err != nil {
			return err
		}

		// Load the CSV file
		results, err := benchexport.LoadCSV[benchexport.SavedResults](resp.Body)
		if err != nil {
			return err
		}

		// save the results to the merged file
		benchexport.SaveAsMarkdown(benchexport.SaveAsMarkdownInput{
			Results:      results,
			CurrentDate:  reportTime,
			InstanceType: instanceType,
			FilePath:     "/tmp/results.md",
		})
	}

	if err != nil {
		return err
	}

	// upload the merged file to the results bucket

	mergedFile, err := os.Open("/tmp/results.md")
	if err != nil {
		return err
	}
	defer mergedFile.Close()

	fmt.Printf("Exporting results to s3://%s/%s.csv\n", event.Bucket, event.Key)

	// upload the merged file to the results bucket
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(event.Bucket),
		Key:    aws.String(event.Key + ".csv"),
		Body:   mergedFile,
	})

	return err
}

var s3Client *s3.S3

func init() {
	sess := session.Must(session.NewSession())
	s3Client = s3.New(sess)
}

func main() {
	lambda.Start(HandleRequest)
}
