package exportresults

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Parameters:
// - bucket: string
// - key: string

type Event struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

func HandleRequest(ctx context.Context, event Event) error {
	// get the results from the results bucket
	// all results are like this: s3://<bucket>/<key>_<instance_type>.csv
	// we want to get all the keys and then download them all into a tmp folder
	// then we merge them into a single file

	// get all the keys from the results bucket
	resp, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(event.Bucket),
		Prefix: aws.String(event.Key),
	})

	header := ""
	mergedContent := ""

	// get only csv files
	for _, obj := range resp.Contents {
		// matches /<key>_<instance_type>.csv
		if regexp.MustCompile(`^.*_.*\.csv$`).MatchString(*obj.Key) {
			// download the file
			resp, err := s3Client.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(event.Bucket),
				Key:    aws.String(*obj.Key),
			})
			if err != nil {
				return err
			}

			content, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			lines := strings.Split(string(content), "\n")

			// get header from first file
			if header == "" {
				header = lines[0]
			}

			mergedContent += strings.Join(lines[1:], "\n") + "\n"
		}
	}

	if err != nil {
		return err
	}

	fmt.Printf("Exporting results to s3://%s/%s.csv\n", event.Bucket, event.Key)
	// TODO: export as markdown table

	// upload the merged file to the results bucket
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(event.Bucket),
		Key:    aws.String(event.Key + ".csv"),
		Body:   strings.NewReader(mergedContent),
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
