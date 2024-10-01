package observer

import (
	"fmt"
	"path"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	kwil_gateway "github.com/truflation/tsn-db/infra/lib/kwil-gateway"
	kwil_indexer_instance "github.com/truflation/tsn-db/infra/lib/kwil-indexer"
	"github.com/truflation/tsn-db/infra/lib/tsn/cluster"
)

type AttachObservabilityInput struct {
	TSNCluster      cluster.TSNCluster
	KGWInstance     kwil_gateway.KGWInstance
	IndexerInstance kwil_indexer_instance.IndexerInstance
}

func AttachObservability(scope constructs.Construct, input *AttachObservabilityInput) {
	// we've been using the same prefix for all observer params to facilitate
	// the ability to attach the same policy to all observer instances
	// if we plan to have different params for envs (dev, test, prod), we'll need to
	// change this
	paramsPrefix := "/tsn/observer/"
	attachObservability := func(
		template awsec2.LaunchTemplate,
		instanceName string,
	) {
		// instantiate params with the ones are already available
		params := ObserverParameters{
			InstanceName: jsii.String(instanceName),
		}

		initScript := GetObserverScript(ObserverScriptInput{
			ZippedAssetsDir: ObserverZipAssetDir,
			Params:          &params,
			Prefix:          paramsPrefix,
		})

		attachSSMReadAccess(
			scope,
			jsii.String(fmt.Sprintf("%s-observer-ssm-policy", instanceName)),
			template.Role(),
			paramsPrefix,
		)

		template.UserData().AddCommands(initScript)
	}

	domain := config.GetDomainStage(scope)
	// if it's empty, we assign prod domain
	if domain == "" {
		domain = "prod"
	}

	type ObservableStructure struct {
		InstanceName   string
		LaunchTemplate awsec2.LaunchTemplate
		InitData       *awsec2.CloudFormationInit
	}

	observableStructures := []ObservableStructure{
		{
			LaunchTemplate: input.KGWInstance.LaunchTemplate,
			InstanceName:   fmt.Sprintf("%s-kgw", domain),
		},
		{
			LaunchTemplate: input.IndexerInstance.LaunchTemplate,
			InstanceName:   fmt.Sprintf("%s-kwil-indexer", domain),
		},
	}

	for _, tsnInstance := range input.TSNCluster.Nodes {
		observableStructures = append(observableStructures, ObservableStructure{
			InstanceName:   *jsii.Sprintf("%s-tsn-node-%d", domain, tsnInstance.Index),
			LaunchTemplate: tsnInstance.LaunchTemplate,
		})
	}

	for _, observableStructure := range observableStructures {
		attachObservability(
			observableStructure.LaunchTemplate,
			observableStructure.InstanceName,
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
	role.AttachInlinePolicy(awsiam.NewPolicy(
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
	))
}
