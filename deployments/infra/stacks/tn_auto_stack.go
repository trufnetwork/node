package stacks

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/config/domain"
	"github.com/trufnetwork/node/infra/lib/constructs/kwil_cluster"
	"github.com/trufnetwork/node/infra/lib/constructs/observability_suite"
	"github.com/trufnetwork/node/infra/lib/constructs/validator_set"
	kwil_network "github.com/trufnetwork/node/infra/lib/kwil-network"
	"github.com/trufnetwork/node/infra/lib/observer"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type TnAutoStackProps struct {
	awscdk.StackProps
	CertStackExports CertStackExports
}

func TnAutoStack(scope constructs.Construct, id string, props *TnAutoStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)
	if !config.IsStackInSynthesis(stack) {
		return stack
	}

	initElements := []awsec2.InitElement{}
	shouldIncludeObserver := config.GetEnvironmentVariables[config.AutoStackEnvironmentVariables](stack).IncludeObserver

	if shouldIncludeObserver {
		observerAsset := observer.GetObserverAsset(stack, jsii.String("observer"))

		initObserver := awsec2.InitFile_FromExistingAsset(jsii.String(observer.ObserverZipAssetDir), observerAsset, &awsec2.InitFileOptions{
			Owner: jsii.String("ec2-user"),
		})

		initElements = append(initElements, initObserver)
	}

	vpc := awsec2.Vpc_FromLookup(stack, jsii.String("VPC"), &awsec2.VpcLookupOptions{IsDefault: jsii.Bool(true)})
	cdkParams := config.NewCDKParams(stack)
	hd := domain.NewHostedDomain(stack, "HostedDomain", &domain.HostedDomainProps{
		Spec: domain.Spec{
			Stage:     domain.StageType(*cdkParams.Stage.ValueAsString()),
			Sub:       "",
			DevPrefix: *cdkParams.DevPrefix.ValueAsString(),
		},
		EdgeCertificate: false,
	})

	nodesConfig := kwil_network.KwilNetworkConfigAssetsFromNumberOfNodes(
		stack,
		kwil_network.KwilAutoNetworkConfigAssetInput{NumberOfNodes: config.NumOfNodes(stack)},
	)

	// TN assets via helper
	tnAssets := validator_set.BuildTNAssets(stack, validator_set.TNAssetOptions{RootDir: utils.GetProjectRootDir()})

	vs := validator_set.NewValidatorSet(stack, "ValidatorSet", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		NodesConfig:  nodesConfig,
		KeyPair:      nil,
		Assets:       tnAssets,
		InitElements: initElements,
	})

	// Kwil Cluster assets via helper
	kwilAssets := kwil_cluster.BuildKwilAssets(stack, kwil_cluster.KwilAssetOptions{
		RootDir:          utils.GetProjectRootDir(),
		BinaryBucketName: "kwil-binaries",
		BinaryKeyPrefix:  "gateway",
	})

	kc := kwil_cluster.NewKwilCluster(stack, "KwilCluster", &kwil_cluster.KwilClusterProps{
		Vpc:           vpc,
		HostedDomain:  hd,
		Cert:          props.CertStackExports.DomainCert,
		CorsOrigins:   cdkParams.CorsAllowOrigins.ValueAsString(),
		SessionSecret: cdkParams.SessionSecret.ValueAsString(),
		ChainId:       jsii.String(config.GetEnvironmentVariables[config.MainEnvironmentVariables](stack).ChainId),
		Validators:    vs.Nodes,
		InitElements:  initElements,
		Assets:        kwilAssets,
	})

	if shouldIncludeObserver {
		_ = observability_suite.NewObservabilitySuite(stack, "ObservabilitySuite", &observability_suite.ObservabilitySuiteProps{
			Vpc:          vpc,
			ValidatorSg:  vs.SecurityGroup,
			GatewaySg:    kc.Gateway.SecurityGroup,
			ParamsPrefix: jsii.String(fmt.Sprintf("/%s/observer", *cdkParams.Stage.ValueAsString())),
		})
	}

	return stack
}
