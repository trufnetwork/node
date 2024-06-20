package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/cfn"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/mitchellh/mapstructure"
	init_system_contract "github.com/truflation/tsn-db/internal/init-system-contract"
	"io"
	"time"
)

// DeployContractResourceProperties represents the properties of the custom resource
// must match what is described on our defined CustomResource
type DeployContractResourceProperties struct {
	PrivateKeySSMId      string `json:"PrivateKeySSMId"`
	ProviderUrl          string `json:"ProviderUrl"`
	SystemContractBucket string `json:"SystemContractBucket"`
	SystemContractKey    string `json:"SystemContractKey"`
}

var ssmClient *ssm.SSM
var s3Client *s3.S3

func HandleRequest(ctx context.Context, event cfn.Event) (string, error) {
	// we handle the request type for the resource
	// we only act uppon resource creation and updates
	switch event.RequestType {
	case cfn.RequestCreate, cfn.RequestUpdate:
		break
	case cfn.RequestDelete:
		return "Delete is not implemented for this resource", nil
	default:
		return "", fmt.Errorf("unknown request type %s", event.RequestType)
	}
	var props DeployContractResourceProperties

	if err := mapstructure.Decode(event.ResourceProperties, &props); err != nil {
		return "", fmt.Errorf("failed to decode event.ResourceProperties: %w", err)
	}

	// Read the private key from SSM
	// TODO use decryption with KMS
	privateKey, err := getSSMParameter(ctx, props.PrivateKeySSMId)
	if err != nil {
		return "", fmt.Errorf("failed to read private key from SSM: %w", err)
	}

	// Read the system contract content from S3
	systemContractContent, err := readS3Object(props.SystemContractBucket, props.SystemContractKey)
	if err != nil {
		return "", fmt.Errorf("failed to read system contract content from S3: %w", err)
	}

	// Initialize system contract
	options := init_system_contract.InitSystemContractOptions{
		RetryTimeout:          10 * time.Minute,
		PrivateKey:            privateKey,
		ProviderUrl:           props.ProviderUrl,
		SystemContractContent: systemContractContent,
	}

	if err := init_system_contract.InitSystemContract(ctx, options); err != nil {
		return "", fmt.Errorf("failed to initialize system contract: %w", err)
	}

	return "System contract successfully deployed", nil
}

func main() {
	// we configure the AWS SDK clients outside of the handler to reuse connections
	sess, err := session.NewSession(&aws.Config{})
	if err != nil {
		panic(fmt.Errorf("failed to create new session: %w", err))
	}

	ssmClient = ssm.New(sess)
	s3Client = s3.New(sess)

	lambda.Start(HandleRequest)
}
func getSSMParameter(ctx context.Context, parameterName string) (string, error) {
	// Read the private key from SSM without decryption
	param, err := ssmClient.GetParameterWithContext(ctx, &ssm.GetParameterInput{
		Name: aws.String(parameterName),
	})
	if err != nil {
		return "", err
	}
	return *param.Parameter.Value, nil
}

func readS3Object(bucket string, key string) (string, error) {
	// Read the system contract content from S3
	systemContractObject, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})

	if err != nil {
		return "", fmt.Errorf("failed to read system contract content from S3: %w", err)
	}

	// Read the system contract content
	systemContractContent, err := io.ReadAll(systemContractObject.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read system contract content: %w", err)
	}

	return string(systemContractContent), nil
}
