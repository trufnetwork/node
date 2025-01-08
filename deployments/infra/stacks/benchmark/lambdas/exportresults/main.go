package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/trufnetwork/node/internal/benchmark/benchexport"
)

// -------------------------------------------------------------------------------------------------
// Export Results Lambda
// - takes a list of CSV files from the results bucket, merges them into a single file and saves them as a markdown file and a CSV file
// - both files are then saved back to the results bucket
// - errors if there's no CSV files to process
// -------------------------------------------------------------------------------------------------

// Parameters:
// - bucket: string
// - key: string

type Event struct {
	Bucket string `json:"bucket"`
	// <timestamp> in 2024-08-28T21:10:57.926Z format
	KeyPrefix string `json:"keyPrefix"`
}

const markdownFilePath = "/tmp/results.md"
const csvFilePath = "/tmp/results.csv"

func HandleRequest(ctx context.Context, event Event) error {
	// delete if file exists. remember that lambdas can share the same filesystem accross multiple invocations1
	cleanup()

	log.Printf("Starting export process for bucket: %s, key: %s", event.Bucket, event.KeyPrefix)

	reportTime, err := time.Parse("2006-01-02T15:04:05.000Z", event.KeyPrefix)
	if err != nil {
		log.Printf("Error parsing report time: %v", err)
		return err
	}

	// get all the keys from the results bucket
	resp, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(event.Bucket),
		Prefix: aws.String(event.KeyPrefix),
	})
	if err != nil {
		log.Printf("Error listing objects: %v", err)
		return err
	}

	csvFiles := make([]string, 0)

	// get only csv files
	for _, obj := range resp.Contents {
		log.Printf("Found CSV file: %s", *obj.Key)
		// matches /<key>_<instance_type>.csv
		// example 2024-08-28T21:10:57.926Z_t3.micro.csv
		if regexp.MustCompile(`^.*_.*\.csv$`).MatchString(*obj.Key) {
			log.Printf("Adding CSV file: %s", *obj.Key)
			csvFiles = append(csvFiles, *obj.Key)
		}

	}

	if len(csvFiles) == 0 {
		log.Printf("No CSV files to process")
		return errors.New("no CSV files to process")
	}

	log.Printf("Found %d CSV files to process", len(csvFiles))

	// sort csv files
	sort.Strings(csvFiles)

	// download all the csv files
	for i, csvFile := range csvFiles {
		log.Printf("Processing file %d/%d: %s", i+1, len(csvFiles), csvFile)
		// get instance type from the key
		instanceType := csvFile[strings.LastIndex(csvFile, "_")+1:]
		// remove the .csv extension
		instanceType = instanceType[:strings.LastIndex(instanceType, ".csv")]

		if err != nil {
			log.Printf("Error processing file %s: %v", csvFile, err)
			return err
		}

		// download the file
		resp, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(event.Bucket),
			Key:    aws.String(csvFile),
		})

		if err != nil {
			log.Printf("Error processing file %s: %v", csvFile, err)
			return err
		}

		// Load the CSV file
		results, err := benchexport.LoadCSV[benchexport.SavedResults](resp.Body)
		if err != nil {
			log.Printf("Error processing file %s: %v", csvFile, err)
			return err
		}

		// save the results to the merged file
		err = benchexport.SaveAsMarkdown(benchexport.SaveAsMarkdownInput{
			Results:      results,
			CurrentDate:  reportTime,
			InstanceType: instanceType,
			FilePath:     markdownFilePath,
		})

		err = benchexport.SaveOrAppendToCSV(results, csvFilePath)
		if err != nil {
			log.Printf("Error processing file %s: %v", csvFile, err)
			return err
		}

		if err != nil {
			log.Printf("Error processing file %s: %v", csvFile, err)
			return err
		}
	}

	log.Printf("Exporting results to s3://%s/%s.md", event.Bucket, event.KeyPrefix)

	resultsKey := fmt.Sprintf("reports/%s.md", event.KeyPrefix)
	csvResultsKey := fmt.Sprintf("reports/%s.csv", event.KeyPrefix)

	mergedMdFile, err := os.ReadFile(markdownFilePath)
	if err != nil {
		log.Printf("Error reading merged file: %v", err)
		return err
	}

	mergedCsvFile, err := os.ReadFile(csvFilePath)
	if err != nil {
		log.Printf("Error reading merged file: %v", err)
		return err
	}

	// upload the merged file to the results bucket
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(event.Bucket),
		Key:         aws.String(resultsKey),
		Body:        bytes.NewReader(mergedMdFile),
		ContentType: aws.String("text/markdown"),
	})

	if err != nil {
		log.Printf("Error uploading markdown file: %v", err)
		return err
	}

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(event.Bucket),
		Key:         aws.String(csvResultsKey),
		Body:        bytes.NewReader(mergedCsvFile),
		ContentType: aws.String("text/csv"),
	})

	if err != nil {
		log.Printf("Error uploading CSV file: %v", err)
		return err
	}

	log.Println("Export process completed successfully")
	return nil
}

var s3Client *s3.S3

func init() {
	sess := session.Must(session.NewSession())
	s3Client = s3.New(sess)
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func cleanup() {
	_, err := os.Stat(markdownFilePath)
	if err == nil {
		err = os.Remove(markdownFilePath)
		if err != nil {
			log.Printf("Error deleting file: %v", err)
		}
	}
}

func main() {
	lambda.Start(HandleRequest)
}
