package observer

import (
	"fmt"
	"path"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/lib/constructs/kwil_cluster"
	"github.com/trufnetwork/node/infra/lib/constructs/validator_set"
)

// AttachObservabilityInput defines the inputs for attaching observer components.
type AttachObservabilityInput struct {
	Scope         constructs.Construct        // Changed from AttachObserverPermissionsInput
	ValidatorSet  *validator_set.ValidatorSet // Changed from AttachObserverPermissionsInput
	KwilCluster   *kwil_cluster.KwilCluster   // Changed from AttachObserverPermissionsInput
	ObserverAsset awss3assets.Asset
	// SsmPrefix is now derived internally based on scope/stage
	Params config.CDKParams
}

// ObservableStructure groups resources that need observer attached.
type ObservableStructure struct {
	InstanceName   string
	ServiceName    string
	LaunchTemplate awsec2.LaunchTemplate
	Role           awsiam.IRole
}

// AttachObservability attaches observer components (Vector agent, scripts)
// to the launch templates and grants necessary permissions.
func AttachObservability(input AttachObservabilityInput) {
	// Derive SSM prefix internally
	stage := config.GetStage(input.Scope)
	devPrefix := config.GetDevPrefix(input.Scope)
	envName := string(stage)
	ssmPrefix := fmt.Sprintf("/tsn/observer/%s/%s", stage, devPrefix)

	// Helper function to attach to a single structure
	attachToNode := func(structure ObservableStructure) {

		// 1. Grant Permissions
		attachSSMReadAccess(
			input.Scope,
			jsii.String(structure.ServiceName+"-ObserverSSMPolicy"), // Unique policy ID per service
			structure.Role,
			ssmPrefix,
		)
		input.ObserverAsset.GrantRead(structure.Role)

		// 2. Prepare UserData Commands
		observerDir := "/home/ec2-user/observer"                       // Target directory for observer assets
		startScriptPath := path.Join(observerDir, "start_observer.sh") // Path for the generated script
		downloadAndUnzipCmd := fmt.Sprintf(
			"aws s3 cp s3://%s/%s %s && unzip -o %s -d %s && chown -R ec2-user:ec2-user %s",
			*input.ObserverAsset.S3BucketName(),
			*input.ObserverAsset.S3ObjectKey(),
			ObserverZipAssetDir, // Source path on instance (where InitFile downloads)
			ObserverZipAssetDir,
			observerDir, // Unzip destination
			observerDir, // Chown target
		)

		// Instantiate params
		params := ObserverParameters{
			InstanceName: jsii.String(structure.InstanceName),
			ServiceName:  jsii.String(structure.ServiceName),
			Env:          jsii.String(envName),
			// Let Prometheus/Logs creds be fetched from SSM by the script
		}

		// Generate the script that fetches SSM params and starts compose
		startObserverScriptContent, err := CreateStartObserverScript(CreateStartObserverScriptInput{
			Params:          &params,
			Prefix:          ssmPrefix,
			ObserverDir:     observerDir,
			StartScriptPath: startScriptPath,
		})
		if err != nil {
			// Use panic with more context as before
			panic(fmt.Errorf("create observer start script: %w", err))
		}

		// 3. Add commands to Launch Template UserData
		lt := structure.LaunchTemplate
		lt.UserData().AddCommands(jsii.String(downloadAndUnzipCmd))
		lt.UserData().AddCommands(jsii.String(startObserverScriptContent))
		lt.UserData().AddCommands(jsii.String(startScriptPath))
	}

	// Gather all structures to attach to
	observableStructures := []ObservableStructure{}

	if input.KwilCluster != nil {
		observableStructures = append(observableStructures, ObservableStructure{
			InstanceName:   fmt.Sprintf("%s-%s-gateway", stage, devPrefix),
			ServiceName:    "gateway",
			LaunchTemplate: input.KwilCluster.Gateway.LaunchTemplate,
			Role:           input.KwilCluster.Gateway.Role,
		})
		observableStructures = append(observableStructures, ObservableStructure{
			InstanceName:   fmt.Sprintf("%s-%s-indexer", stage, devPrefix),
			ServiceName:    "indexer",
			LaunchTemplate: input.KwilCluster.Indexer.LaunchTemplate,
			Role:           input.KwilCluster.Indexer.Role,
		})
	}

	if input.ValidatorSet != nil {
		for _, tsnInstance := range input.ValidatorSet.Nodes {
			observableStructures = append(observableStructures, ObservableStructure{
				InstanceName:   fmt.Sprintf("%s-%s-tn-node-%d", stage, devPrefix, tsnInstance.Index),
				LaunchTemplate: tsnInstance.LaunchTemplate,
				ServiceName:    "tn-node",
				Role:           tsnInstance.Role,
			})
		}
	}

	// Attach to each structure
	for _, structure := range observableStructures {
		attachToNode(structure)
	}
}

// attachSSMReadAccess grants SSM read permissions for a given prefix.
func attachSSMReadAccess(
	scope constructs.Construct,
	id *string, // Unique ID for the policy construct within the scope
	role awsiam.IRole,
	ssmPrefix string, // Changed from paramsPrefix for clarity
) {
	paramResourceName := path.Join("parameter", strings.TrimPrefix(ssmPrefix, "/"), "*") // Use path.Join and trim leading slash
	// Create inline policy under the stack scope using the provided static ID
	policy := awsiam.NewPolicy(
		scope,
		id, // Use the unique ID passed in
		&awsiam.PolicyProps{
			PolicyName: jsii.Sprintf("%s-ssm-observer-read", role.RoleName()), // Optional: Give policy a meaningful name
			Statements: &[]awsiam.PolicyStatement{
				awsiam.NewPolicyStatement(
					&awsiam.PolicyStatementProps{
						Effect:  awsiam.Effect_ALLOW,
						Actions: jsii.Strings("ssm:GetParameter", "ssm:GetParameters"), // Use jsii.Strings
						Resources: jsii.Strings(fmt.Sprintf( // Use jsii.Strings
							"arn:aws:ssm:%s:%s:%s",
							*awscdk.Aws_REGION(),
							*awscdk.Aws_ACCOUNT_ID(),
							paramResourceName,
						)),
					}),
			},
		},
	)
	role.AttachInlinePolicy(policy)
}
