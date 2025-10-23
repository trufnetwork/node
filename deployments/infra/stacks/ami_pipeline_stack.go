package stacks

import (
	_ "embed"
	"encoding/base64"
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsimagebuilder"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
)

//go:embed docker-compose.template.yml
var dockerComposeTemplate string

func getEncodedDockerCompose() string {
	return base64.StdEncoding.EncodeToString([]byte(dockerComposeTemplate))
}

type AmiPipelineStackProps struct {
	awscdk.StackProps
}

type AmiPipelineStackExports struct {
	PipelineArn         string
	DockerComponentArn  string
	ConfigComponentArn  string
	WelcomeComponentArn string
	RecipeArn           string
	InfraConfigArn      string
	DistributionArn     string
	S3BucketName        string
	InstanceProfileArn  string
}

func AmiPipelineStack(scope constructs.Construct, id string, props *AmiPipelineStackProps) (awscdk.Stack, AmiPipelineStackExports) {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)

	if !config.IsStackInSynthesis(stack) {
		return stack, AmiPipelineStackExports{}
	}

	stage := config.GetStage(stack)
	devPrefix := config.GetDevPrefix(stack)

	// Helper function to construct names with dev prefix
	nameWithPrefix := func(name string) string {
		if devPrefix != "" {
			return devPrefix + "-" + name
		}
		return name
	}

	// S3 bucket for AMI build artifacts and logs
	artifactsBucket := awss3.NewBucket(stack, jsii.String("AmiArtifactsBucket"), &awss3.BucketProps{
		RemovalPolicy:     awscdk.RemovalPolicy_DESTROY,
		AutoDeleteObjects: jsii.Bool(true),
		Versioned:         jsii.Bool(false),
		PublicReadAccess:  jsii.Bool(false),
		BlockPublicAccess: awss3.BlockPublicAccess_BLOCK_ALL(),
		Encryption:        awss3.BucketEncryption_S3_MANAGED,
		EnforceSSL:        jsii.Bool(true),
	})

	// IAM role for EC2 Image Builder instance
	imageBuilderRole := awsiam.NewRole(stack, jsii.String("ImageBuilderInstanceRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
		ManagedPolicies: &[]awsiam.IManagedPolicy{
			awsiam.ManagedPolicy_FromAwsManagedPolicyName(jsii.String("EC2InstanceProfileForImageBuilder")),
			awsiam.ManagedPolicy_FromAwsManagedPolicyName(jsii.String("AmazonSSMManagedInstanceCore")),
		},
	})

	// Grant S3 access to the Image Builder role
	artifactsBucket.GrantReadWrite(imageBuilderRole, nil)

	// Instance profile for Image Builder
	instanceProfile := awsiam.NewCfnInstanceProfile(stack, jsii.String("ImageBuilderInstanceProfile"), &awsiam.CfnInstanceProfileProps{
		Roles:               &[]*string{imageBuilderRole.RoleName()},
		InstanceProfileName: jsii.String(nameWithPrefix("tn-ami-instance-profile-" + string(stage))),
	})

	// Infrastructure configuration for Image Builder
	infraConfig := awsimagebuilder.NewCfnInfrastructureConfiguration(stack, jsii.String("AmiInfrastructureConfiguration"), &awsimagebuilder.CfnInfrastructureConfigurationProps{
		Name:                jsii.String(nameWithPrefix("tn-ami-infra-config-" + string(stage))),
		InstanceProfileName: instanceProfile.InstanceProfileName(),
		InstanceTypes: &[]*string{
			jsii.String("t3.medium"), // Cost-effective for AMI building
		},
		InstanceMetadataOptions: &awsimagebuilder.CfnInfrastructureConfiguration_InstanceMetadataOptionsProperty{
			HttpTokens:              jsii.String("required"),
			HttpPutResponseHopLimit: jsii.Number(2),
		},
		// Omit to use default VPC security group
		SecurityGroupIds: nil,
		Logging: &awsimagebuilder.CfnInfrastructureConfiguration_LoggingProperty{
			S3Logs: &awsimagebuilder.CfnInfrastructureConfiguration_S3LogsProperty{
				S3BucketName: artifactsBucket.BucketName(),
				S3KeyPrefix:  jsii.String("ami-build-logs"),
			},
		},
		TerminateInstanceOnFailure: jsii.Bool(true),
		Description:                jsii.String("Infrastructure configuration for TRUF.NETWORK node AMI building"),
	})

	// Component for installing Docker and dependencies
	dockerComponent := awsimagebuilder.NewCfnComponent(stack, jsii.String("DockerInstallComponent"), &awsimagebuilder.CfnComponentProps{
		Name:        jsii.String(nameWithPrefix("tn-docker-install-" + string(stage))),
		Platform:    jsii.String("Linux"),
		Version:     jsii.String("1.0.0"),
		Description: jsii.String("Install Docker, Docker Compose, and TRUF.NETWORK dependencies"),
		Data: jsii.String(`
name: DockerInstallComponent
description: Install Docker, Docker Compose, and TRUF.NETWORK dependencies
schemaVersion: 1.0

phases:
  - name: build
    steps:
      - name: UpdateSystem
        action: ExecuteBash
        inputs:
          commands:
            - sudo apt-get update
            - sudo apt-get upgrade -y

      - name: InstallDocker
        action: ExecuteBash
        inputs:
          commands:
            - sudo apt-get install -y ca-certificates curl gnupg lsb-release
            - sudo mkdir -p /etc/apt/keyrings
            - curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
            - echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            - sudo apt-get update
            - sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

      - name: ConfigureDocker
        action: ExecuteBash
        inputs:
          commands:
            - sudo systemctl enable docker
            - sudo systemctl start docker
            - sudo usermod -aG docker ubuntu

      - name: InstallAdditionalTools
        action: ExecuteBash
        inputs:
          commands:
            - sudo apt-get install -y postgresql-client-16 jq curl wget unzip

      - name: CreateTNUser
        action: ExecuteBash
        inputs:
          commands:
            - sudo useradd -m -s /bin/bash tn || true
            - sudo usermod -aG docker tn
            - sudo mkdir -p /opt/tn
            - sudo chown tn:tn /opt/tn

      - name: VerifyInstallation
        action: ExecuteBash
        inputs:
          commands:
            - docker --version
            - docker-compose --version
            - systemctl is-active docker
`),
	})

	// Component 1: TRUF.NETWORK configuration and scripts
	configComponent := awsimagebuilder.NewCfnComponent(stack, jsii.String("TNConfigComponent"), &awsimagebuilder.CfnComponentProps{
		Name:        jsii.String(nameWithPrefix("tn-config-setup-" + string(stage))),
		Platform:    jsii.String("Linux"),
		Version:     jsii.String("1.0.0"),
		Description: jsii.String("Set up TRUF.NETWORK configuration structure and scripts"),
		Data: jsii.String(`
name: TNConfigComponent
description: Set up TRUF.NETWORK configuration structure and scripts
schemaVersion: 1.0

phases:
  - name: build
    steps:
      - name: CreateDirectoryStructure
        action: ExecuteBash
        inputs:
          commands:
            - sudo mkdir -p /opt/tn/{configs/network/v2,data}
            - sudo chown -R tn:tn /opt/tn

      - name: DownloadNetworkConfigs
        action: ExecuteBash
        inputs:
          commands:
            - cd /tmp
            - curl -fsSL https://raw.githubusercontent.com/trufnetwork/truf-node-operator/main/configs/network/v2/genesis.json -o genesis.json
            - sudo mv genesis.json /opt/tn/configs/network/v2/
            - sudo chown -R tn:tn /opt/tn/configs

      - name: CreateDockerComposeFile
        action: ExecuteBash
        inputs:
          commands:
            - |
              echo "` + getEncodedDockerCompose() + `" | base64 -d > /opt/tn/docker-compose.yml
            - sudo chown tn:tn /opt/tn/docker-compose.yml
            - sudo chmod 644 /opt/tn/docker-compose.yml

      - name: CreateSystemdService
        action: ExecuteBash
        inputs:
          commands:
            - |
              sudo tee /etc/systemd/system/tn-node.service > /dev/null << 'EOF'
              [Unit]
              Description=TRUF.NETWORK Node
              After=docker.service
              Requires=docker.service

              [Service]
              Type=oneshot
              RemainAfterExit=true
              WorkingDirectory=/opt/tn
              ExecStart=/usr/bin/docker compose up -d
              ExecStop=/usr/bin/docker compose down
              User=tn
              Group=tn

              [Install]
              WantedBy=multi-user.target
              EOF

      - name: CreateConfigurationScript
        action: ExecuteBash
        inputs:
          commands:
            - |
              sudo tee /usr/local/bin/tn-node-configure > /dev/null << 'EOF'
              #!/bin/bash
              set -e
              PRIVATE_KEY=""
              ENABLE_MCP=false
              NETWORK=""
              while [[ $# -gt 0 ]]; do
                case $1 in
                  --private-key) PRIVATE_KEY="$2"; shift 2;;
                  --enable-mcp) ENABLE_MCP=true; shift;;
                  --network) NETWORK="$2"; shift 2;;
                  *) echo "Unknown option"; exit 1;;
                esac
              done
              RECONFIGURE=false
              [ -f /opt/tn/.env ] && RECONFIGURE=true
              if [ -n "$NETWORK" ]; then
                CHAIN_ID="$NETWORK"
                NETWORK_TYPE="custom"
              else
                CHAIN_ID="tn-v2.1"
                NETWORK_TYPE="mainnet"
              fi
              cd /opt/tn
              if [ "$RECONFIGURE" = true ]; then
                [ -n "$PRIVATE_KEY" ] && { echo "Error: Cannot change key"; exit 1; }
                [ -n "$NETWORK" ] && { echo "Error: Cannot change network"; exit 1; }
                grep -q "TN_PRIVATE_KEY=" .env && EXISTING_KEY=$(grep "TN_PRIVATE_KEY=" .env | cut -d'=' -f2)
                grep -q "CHAIN_ID=" .env && CHAIN_ID=$(grep "CHAIN_ID=" .env | cut -d'=' -f2)
                grep -q "NETWORK_TYPE=" .env && NETWORK_TYPE=$(grep "NETWORK_TYPE=" .env | cut -d'=' -f2)
                sudo systemctl stop tn-node || true
                sudo -u tn docker compose down || true
              fi
              cat > .env << ENVEOF
              CHAIN_ID=$CHAIN_ID
              NETWORK_TYPE=$NETWORK_TYPE
              ENVEOF
              [ "$ENABLE_MCP" = true ] && echo "COMPOSE_PROFILES=mcp" >> .env
              if [ "$RECONFIGURE" = false ] && [ -n "$PRIVATE_KEY" ]; then
                echo "TN_PRIVATE_KEY=$PRIVATE_KEY" >> .env
              elif [ "$RECONFIGURE" = true ] && [ -n "$EXISTING_KEY" ]; then
                echo "TN_PRIVATE_KEY=$EXISTING_KEY" >> .env
              fi
              sudo chown tn:tn .env
              sudo chmod 600 .env
              sudo systemctl daemon-reload
              sudo systemctl enable tn-node
              sudo systemctl start tn-node
              MAX_WAIT=60
              ELAPSED=0
              while [ $ELAPSED -lt $MAX_WAIT ]; do
                sudo -u tn docker compose ps --status running 2>/dev/null | grep -q tn-node && break
                sleep 2
                ELAPSED=$((ELAPSED + 2))
              done
              echo "Done. Status: $(sudo systemctl is-active tn-node)"
              [ "$ENABLE_MCP" = true ] && echo "MCP: http://$(curl -s --max-time 2 http://169.254.169.254/latest/meta-data/public-ipv4 || echo localhost):8000/sse"
              EOF
            - sudo chmod +x /usr/local/bin/tn-node-configure

      - name: CreateUpdateScript
        action: ExecuteBash
        inputs:
          commands:
            - |
              sudo tee /usr/local/bin/tn-node-update > /dev/null << 'EOF'
              #!/bin/bash
              set -e
              cd /opt/tn
              sudo -u tn docker compose pull
              sudo -u tn docker compose up -d --force-recreate
              echo "Updated: $(sudo systemctl is-active tn-node)"
              EOF
            - sudo chmod +x /usr/local/bin/tn-node-update

      - name: PrepareServices
        action: ExecuteBash
        inputs:
          commands:
            - sudo systemctl daemon-reload
`),
	})

	// Component 2: Welcome messages (split to stay under 16KB limit)
	welcomeComponent := awsimagebuilder.NewCfnComponent(stack, jsii.String("TNWelcomeComponent"), &awsimagebuilder.CfnComponentProps{
		Name:        jsii.String(nameWithPrefix("tn-welcome-setup-" + string(stage))),
		Platform:    jsii.String("Linux"),
		Version:     jsii.String("1.0.0"),
		Description: jsii.String("Set up TRUF.NETWORK welcome messages and user guidance"),
		Data: jsii.String(`
name: TNWelcomeComponent
description: Set up TRUF.NETWORK welcome messages and user guidance
schemaVersion: 1.0

phases:
  - name: build
    steps:
      - name: CreateWelcomeMessage
        action: ExecuteBash
        inputs:
          commands:
            - |
              sudo tee /etc/motd > /dev/null << 'EOF'

              🚀 Welcome to your TRUF.NETWORK Node!

              Your node is ready for configuration. Please run ONE of the following commands:

              # Basic setup (auto-generated private key, no MCP)
              sudo tn-node-configure

              # With your own private key
              sudo tn-node-configure --private-key "your-64-character-hex-key"

              # With MCP enabled for AI integration
              sudo tn-node-configure --enable-mcp

              # Full configuration example
              sudo tn-node-configure \
                --private-key "your-key" \
                --enable-mcp

              After configuration, your node will start automatically and begin syncing!

              📦 To update to the latest software:
              sudo tn-node-update

              🔒 Security Group Ports:
              - Port 6600 (P2P): Recommended for two-way peer connections
              - Port 8484 (RPC): Needed for public node access

              🤖 MCP (AI Integration) - Optional:
              1. Configure: sudo tn-node-configure --enable-mcp
              2. Open port 8000 in Security Group
              3. Access: http://YOUR-PUBLIC-IP:8000/sse
              EOF

            - |
              # Create a one-time welcome script that shows on first SSH login
              sudo tee /etc/profile.d/tn-welcome.sh > /dev/null << 'EOF'
              #!/bin/bash

              # Only show welcome message if node is not configured yet
              if [ ! -f /opt/tn/.env ] && [ "$USER" = "ubuntu" ]; then
                echo ""
                echo "🚀 Welcome to your TRUF.NETWORK Node!"
                echo ""
                echo "Your node is ready for configuration. Please run ONE of the following commands:"
                echo ""
                echo "# Basic setup (auto-generated private key, no MCP)"
                echo "sudo tn-node-configure"
                echo ""
                echo "# With your own private key"
                echo "sudo tn-node-configure --private-key \"your-64-character-hex-key\""
                echo ""
                echo "# With MCP enabled for AI integration"
                echo "sudo tn-node-configure --enable-mcp"
                echo ""
                echo "# Full configuration example"
                echo "sudo tn-node-configure \\"
                echo "  --private-key \"your-key\" \\"
                echo "  --enable-mcp"
                echo ""
                echo "After configuration, your node will start automatically and begin syncing!"
                echo ""
                echo "📦 To update to the latest software:"
                echo "sudo tn-node-update"
                echo ""
                echo "🔒 Security Group Ports:"
                echo "- Port 6600 (P2P): Recommended for two-way peer connections"
                echo "- Port 8484 (RPC): Needed for public node access"
                echo ""
                echo "🤖 MCP (AI Integration) - Optional:"
                echo "1. Configure: sudo tn-node-configure --enable-mcp"
                echo "2. Open port 8000 in Security Group"
                echo "3. Access: http://YOUR-PUBLIC-IP:8000/sse"
              fi
              EOF
            - sudo chmod +x /etc/profile.d/tn-welcome.sh
`),
	})

	// Add explicit dependency
	infraConfig.AddDependency(instanceProfile)

	// Image recipe that combines all components
	imageRecipe := awsimagebuilder.NewCfnImageRecipe(stack, jsii.String("TNAmiRecipe"), &awsimagebuilder.CfnImageRecipeProps{
		Name:        jsii.String(nameWithPrefix("tn-ami-recipe-" + string(stage))),
		Version:     jsii.String("1.0.0"),
		ParentImage: awscdk.Fn_Sub(jsii.String("{{resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id}}"), nil),
		Description: jsii.String("TRUF.NETWORK node AMI with Docker infrastructure"),
		Components: &[]*awsimagebuilder.CfnImageRecipe_ComponentConfigurationProperty{
			{
				ComponentArn: dockerComponent.AttrArn(),
			},
			{
				ComponentArn: configComponent.AttrArn(),
			},
			{
				ComponentArn: welcomeComponent.AttrArn(),
			},
		},
		BlockDeviceMappings: &[]*awsimagebuilder.CfnImageRecipe_InstanceBlockDeviceMappingProperty{
			{
				DeviceName: jsii.String("/dev/sda1"),
				Ebs: &awsimagebuilder.CfnImageRecipe_EbsInstanceBlockDeviceSpecificationProperty{
					VolumeSize:          jsii.Number(20), // 20GB root volume
					VolumeType:          jsii.String("gp3"),
					Encrypted:           jsii.Bool(false), // AWS Marketplace prohibits encrypted AMIs
					DeleteOnTermination: jsii.Bool(true),
				},
			},
		},
	})

	// Distribution configuration for current region
	distributionConfig := awsimagebuilder.NewCfnDistributionConfiguration(stack, jsii.String("AmiDistributionConfiguration"), &awsimagebuilder.CfnDistributionConfigurationProps{
		Name:        jsii.String(nameWithPrefix("tn-ami-distribution-" + string(stage))),
		Description: jsii.String("Distribution configuration for TRUF.NETWORK AMI"),
		Distributions: &[]*awsimagebuilder.CfnDistributionConfiguration_DistributionProperty{
			{
				Region: awscdk.Aws_REGION(), // Use current region token
				AmiDistributionConfiguration: &awsimagebuilder.CfnDistributionConfiguration_AmiDistributionConfigurationProperty{
					AmiTags: &map[string]*string{
						"Name": jsii.String("TRUFNETWORK-Node-{{imagebuilder:buildDate}}"),
					},
				},
			},
		},
	})

	// Image pipeline that orchestrates the entire process
	imagePipeline := awsimagebuilder.NewCfnImagePipeline(stack, jsii.String("TNAmiPipeline"), &awsimagebuilder.CfnImagePipelineProps{
		Name:                           jsii.String(nameWithPrefix("tn-ami-pipeline-" + string(stage))),
		Description:                    jsii.String("Automated AMI building pipeline for TRUF.NETWORK nodes"),
		ImageRecipeArn:                 imageRecipe.AttrArn(),
		InfrastructureConfigurationArn: infraConfig.AttrArn(),
		DistributionConfigurationArn:   distributionConfig.AttrArn(),
		Status:                         jsii.String("ENABLED"),
		ImageTestsConfiguration: &awsimagebuilder.CfnImagePipeline_ImageTestsConfigurationProperty{
			ImageTestsEnabled: jsii.Bool(true),
			TimeoutMinutes:    jsii.Number(90),
		},
	})

	// Output important ARNs and names for GitHub Actions
	awscdk.NewCfnOutput(stack, jsii.String("AmiPipelineArnOutput"), &awscdk.CfnOutputProps{
		Value:       imagePipeline.AttrArn(),
		Description: jsii.String("ARN of the AMI building pipeline"),
		ExportName:  jsii.String(nameWithPrefix("AmiPipelineArn-" + string(stage))),
	})

	awscdk.NewCfnOutput(stack, jsii.String("AmiArtifactsBucketOutput"), &awscdk.CfnOutputProps{
		Value:       artifactsBucket.BucketName(),
		Description: jsii.String("S3 bucket for AMI build artifacts"),
		ExportName:  jsii.String(nameWithPrefix("AmiArtifactsBucket-" + string(stage))),
	})

	exports := AmiPipelineStackExports{
		PipelineArn:         *imagePipeline.AttrArn(),
		DockerComponentArn:  *dockerComponent.AttrArn(),
		ConfigComponentArn:  *configComponent.AttrArn(),
		WelcomeComponentArn: *welcomeComponent.AttrArn(),
		RecipeArn:           *imageRecipe.AttrArn(),
		InfraConfigArn:      *infraConfig.AttrArn(),
		DistributionArn:     *distributionConfig.AttrArn(),
		S3BucketName:        *artifactsBucket.BucketName(),
		InstanceProfileArn:  *instanceProfile.AttrArn(),
	}

	return stack, exports
}
