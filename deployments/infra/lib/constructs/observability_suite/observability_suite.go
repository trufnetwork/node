package observability_suite

import (
	"fmt"
	"path"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	jsii "github.com/aws/jsii-runtime-go"

	"github.com/trufnetwork/node/infra/lib/observer"
	"github.com/trufnetwork/node/infra/lib/utils"
)

// ObservabilitySuiteProps holds inputs for creating an observability suite
// ValidatorSg and GatewaySg are used to allow Vector ingress
// ParamsPrefix defines the SSM prefix under which to write parameters

type ObservabilitySuiteProps struct {
	Vpc          awsec2.IVpc
	ValidatorSg  awsec2.ISecurityGroup
	GatewaySg    awsec2.ISecurityGroup
	ParamsPrefix *string
}

// ObservabilitySuite is a reusable construct deploying Vector and SSM parameters

type ObservabilitySuite struct {
	constructs.Construct

	VectorInstance awsec2.Instance
	ParamPaths     []*string
}

// NewObservabilitySuite provisions the observability suite: packages assets, writes SSM params, and sets up SG rules

func NewObservabilitySuite(scope constructs.Construct, id string, props *ObservabilitySuiteProps) *ObservabilitySuite {
	node := constructs.NewConstruct(scope, jsii.String(id))
	os := &ObservabilitySuite{Construct: node}

	// write parameters to SSM
	descs, err := utils.GetParameterDescriptors(&observer.ObserverParameters{})
	if err != nil {
		panic(fmt.Errorf("failed to get parameter descriptors: %w", err))
	}
	var paths []*string
	for _, desc := range descs {
		if desc.IsSSMParameter {
			name := path.Join(*props.ParamsPrefix, desc.SSMPath)
			paths = append(paths, jsii.String(name))
		}
	}
	os.ParamPaths = paths

	// configure security group for observer
	sg := awsec2.NewSecurityGroup(node, jsii.String("ObserverSG"), &awsec2.SecurityGroupProps{
		Vpc:              props.Vpc,
		AllowAllOutbound: jsii.Bool(true),
		Description:      jsii.String("Observability suite security group"),
	})
	// allow metrics from validators and gateway
	sg.AddIngressRule(
		props.ValidatorSg,
		awsec2.Port_AllTcp(),
		jsii.String("Allow validator metrics"),
		jsii.Bool(false),
	)
	sg.AddIngressRule(
		props.GatewaySg,
		awsec2.Port_AllTcp(),
		jsii.String("Allow gateway metrics"),
		jsii.Bool(false),
	)

	// package observer assets
	asset := observer.GetObserverAsset(node, jsii.String("ObserverAsset"))
	// prepare init elements to unpack zip
	initElems := []awsec2.InitElement{
		awsec2.InitFile_FromExistingAsset(jsii.String(observer.ObserverZipAssetDir), asset, &awsec2.InitFileOptions{Owner: jsii.String("ec2-user")}),
	}
	// create EC2 instance for Vector
	initConfig := awsec2.CloudFormationInit_FromElements(initElems...)
	vectorInst := awsec2.NewInstance(node, jsii.String("ObserverInstance"), &awsec2.InstanceProps{
		Vpc:           props.Vpc,
		InstanceType:  awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_SMALL),
		MachineImage:  awsec2.MachineImage_LatestAmazonLinux2(nil),
		SecurityGroup: sg,
		Init:          initConfig,
	})
	// add startup script commands
	script := observer.GetObserverScript(observer.ObserverScriptInput{
		ZippedAssetsDir: observer.ObserverZipAssetDir,
		Params:          &observer.ObserverParameters{},
		Prefix:          *props.ParamsPrefix,
	})
	vectorInst.UserData().AddCommands(script)
	os.VectorInstance = vectorInst

	return os
}
