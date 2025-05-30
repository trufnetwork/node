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
	"github.com/trufnetwork/node/infra/lib/cdklogger"
	"github.com/trufnetwork/node/infra/lib/constructs/kwil_cluster"
	"github.com/trufnetwork/node/infra/lib/constructs/validator_set"
	"github.com/trufnetwork/node/infra/lib/utils"
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
	InstanceName       string
	UniquePolicyPrefix string
	ServiceName        string
	LaunchTemplate     awsec2.LaunchTemplate
	Role               awsiam.IRole
}

// AttachObservability attaches observer components (Vector agent, scripts)
// to the launch templates and grants necessary permissions.
func AttachObservability(input AttachObservabilityInput) {
	// Derive SSM prefix internally
	stage := config.GetStage(input.Scope)
	devPrefix := config.GetDevPrefix(input.Scope)
	envName := string(stage)
	ssmPrefix := "/tsn/observer"

	// Helper function to attach to a single structure
	attachToNode := func(structure ObservableStructure) {

		// 1. Grant Permissions
		attachSSMReadAccess(
			input.Scope,
			jsii.String(structure.UniquePolicyPrefix+"-ObserverSSMPolicy"),
			structure.UniquePolicyPrefix,
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
			AwsRegion:       *awscdk.Aws_REGION(),
		})
		if err != nil {
			// Use panic with more context as before
			panic(fmt.Errorf("create observer start script: %w", err))
		}

		// Log before adding commands
		// Use structure.InstanceName in the logger's constructID to make the path specific
		// e.g., /<StackName>/my-dev-tn-node-0/UserData/[ObserverSetup] ...
		userDataLogConstructID := structure.InstanceName + "/UserData"
		cdklogger.LogInfo(input.Scope, userDataLogConstructID, "[ObserverSetup] Adding observer asset download, script generation, and service start commands for instance %s (service: %s).", structure.InstanceName, structure.ServiceName)

		// 3. Add commands to Launch Template UserData
		lt := structure.LaunchTemplate
		lt.UserData().AddCommands(jsii.String(downloadAndUnzipCmd))
		lt.UserData().AddCommands(jsii.String(startObserverScriptContent))

		// Create systemd service for the observer
		systemdServiceScript := utils.CreateSystemdServiceScript(
			"observer",
			"Observer Compose",
			startScriptPath,
			fmt.Sprintf("/bin/bash -c \"docker compose -f %s/observer-compose.yml down\"", observerDir),
			nil, // No additional environment variables needed since they're handled by the start script
		)
		lt.UserData().AddCommands(jsii.String(systemdServiceScript))
	}

	// Gather all structures to attach to
	observableStructures := []ObservableStructure{}

	if input.KwilCluster != nil {
		observableStructures = append(observableStructures, ObservableStructure{
			InstanceName:       fmt.Sprintf("%s-%s-gateway", stage, devPrefix),
			UniquePolicyPrefix: "Gateway",
			ServiceName:        "gateway",
			LaunchTemplate:     input.KwilCluster.Gateway.LaunchTemplate,
			Role:               input.KwilCluster.Gateway.Role,
		})
		observableStructures = append(observableStructures, ObservableStructure{
			InstanceName:       fmt.Sprintf("%s-%s-indexer", stage, devPrefix),
			UniquePolicyPrefix: "Indexer",
			ServiceName:        "indexer",
			LaunchTemplate:     input.KwilCluster.Indexer.LaunchTemplate,
			Role:               input.KwilCluster.Indexer.Role,
		})
	}

	if input.ValidatorSet != nil {
		for _, tsnInstance := range input.ValidatorSet.Nodes {
			observableStructures = append(observableStructures, ObservableStructure{
				InstanceName:       fmt.Sprintf("%s-%s-tn-node-%d", stage, devPrefix, tsnInstance.Index),
				UniquePolicyPrefix: fmt.Sprintf("TNNode@%d", tsnInstance.Index),
				LaunchTemplate:     tsnInstance.LaunchTemplate,
				ServiceName:        "tn-node",
				Role:               tsnInstance.Role,
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
	policyPrefix string,
	role awsiam.IRole,
	ssmPrefix string,
) {
	paramResourceName := path.Join("parameter", strings.TrimPrefix(ssmPrefix, "/"), "*") // Use path.Join and trim leading slash
	// Create inline policy under the stack scope using the provided static ID
	policy := awsiam.NewPolicy(
		scope,
		id, // Use the unique ID passed in
		&awsiam.PolicyProps{
			PolicyName: jsii.Sprintf("%s-ssm-observer-read", policyPrefix), // Optional: Give policy a meaningful name
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
