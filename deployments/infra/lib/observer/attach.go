package observer

import (
	"fmt"
	"path"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	kwil_gateway "github.com/trufnetwork/node/infra/lib/kwil-gateway"
	kwil_indexer_instance "github.com/trufnetwork/node/infra/lib/kwil-indexer"
	"github.com/trufnetwork/node/infra/lib/tsn/cluster"
)

type AttachObservabilityInput struct {
	TSNCluster      cluster.TSNCluster
	KGWInstance     kwil_gateway.KGWInstance
	IndexerInstance kwil_indexer_instance.IndexerInstance
}

func AttachObservability(scope constructs.Construct, input *AttachObservabilityInput) {
	// we've been using the same prefix for all observer params
	paramsPrefix := "/tsn/observer/"

	// Determine stage via CDK parameters and DomainConfig
	cdkParams := config.NewCDKParams(scope)
	stageToken := cdkParams.Stage.ValueAsString()
	stage := domaincfg.StageType(*stageToken)
	envName := string(stage)

	attachObservability := func(
		template awsec2.LaunchTemplate,
		instanceName string,
		serviceName string,
	) {
		// instantiate params with the ones are already available
		params := ObserverParameters{
			InstanceName: jsii.String(instanceName),
			ServiceName:  jsii.String(serviceName),
			Env:          jsii.String(envName),
		}

		initScript := GetObserverScript(ObserverScriptInput{
			ZippedAssetsDir: ObserverZipAssetDir,
			Params:          &params,
			Prefix:          paramsPrefix,
		})

		// Attach SSM read policy using the serviceName (static) for policy ID
		attachSSMReadAccess(
			scope,
			jsii.String(*params.ServiceName+"-ObserverSSMPolicy"),
			template.Role(),
			paramsPrefix,
		)

		template.UserData().AddCommands(initScript)
	}

	type ObservableStructure struct {
		InstanceName   string
		ServiceName    string
		LaunchTemplate awsec2.LaunchTemplate
		InitData       *awsec2.CloudFormationInit
	}

	observableStructures := []ObservableStructure{
		{
			LaunchTemplate: input.KGWInstance.LaunchTemplate,
			InstanceName:   fmt.Sprintf("%s-kgw", envName),
			ServiceName:    "kwil-gateway",
		},
		{
			LaunchTemplate: input.IndexerInstance.LaunchTemplate,
			InstanceName:   fmt.Sprintf("%s-kwil-indexer", envName),
			ServiceName:    "kwil-indexer",
		},
	}

	for _, tsnInstance := range input.TSNCluster.Nodes {
		observableStructures = append(observableStructures, ObservableStructure{
			InstanceName:   fmt.Sprintf("%s-tsn-node-%d", envName, tsnInstance.Index),
			LaunchTemplate: tsnInstance.LaunchTemplate,
			ServiceName:    "tsn-node",
		})
	}

	for _, observableStructure := range observableStructures {
		attachObservability(
			observableStructure.LaunchTemplate,
			observableStructure.InstanceName,
			observableStructure.ServiceName,
		)
	}
}

func attachSSMReadAccess(
	scope constructs.Construct,
	id *string,
	role awsiam.IRole,
	paramsPrefix string,
) {
	paramString := path.Join("parameter", paramsPrefix, "*")
	// Create inline policy under the stack scope using the provided static ID
	policy := awsiam.NewPolicy(
		scope,
		id,
		&awsiam.PolicyProps{
			Statements: &[]awsiam.PolicyStatement{
				awsiam.NewPolicyStatement(
					&awsiam.PolicyStatementProps{
						Effect:  awsiam.Effect_ALLOW,
						Actions: &[]*string{jsii.String("ssm:GetParameter"), jsii.String("ssm:GetParameters")},
						Resources: &[]*string{jsii.String(fmt.Sprintf(
							"arn:aws:ssm:%s:%s:%s",
							*awscdk.Aws_REGION(),
							*awscdk.Aws_ACCOUNT_ID(),
							paramString,
						))},
					}),
			},
		},
	)
	role.AttachInlinePolicy(policy)
}
