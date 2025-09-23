package stacks

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsimagebuilder"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
)

type AmiPipelineStackProps struct {
	awscdk.StackProps
}

type AmiPipelineStackExports struct {
	PipelineArn         string
	DockerComponentArn  string
	ConfigComponentArn  string
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

      - name: InstallDockerCompose
        action: ExecuteBash
        inputs:
          commands:
            - sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
            - sudo chmod +x /usr/local/bin/docker-compose
            - sudo ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose

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

	// Component for TRUF.NETWORK configuration
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
            - sudo mkdir -p /opt/tn/{configs/{mainnet,testnet},scripts,data}
            - sudo mkdir -p /opt/tn/configs/mainnet
            - sudo mkdir -p /opt/tn/configs/testnet
            - sudo chown -R tn:tn /opt/tn

      - name: CreateDockerComposeFile
        action: ExecuteBash
        inputs:
          commands:
            - |
              cat > /opt/tn/docker-compose.yml << 'EOF'
              services:
                kwil-postgres:
                  image: kwildb/postgres:16.8-1
                  container_name: tn-postgres
                  environment:
                    POSTGRES_DB: kwild
                    POSTGRES_USER: kwild
                    POSTGRES_PASSWORD: kwild
                  volumes:
                    - postgres_data:/var/lib/postgresql/data
                  ports:
                    - "5432:5432"
                  networks:
                    - tn-network
                  restart: unless-stopped
                  healthcheck:
                    test: ["CMD-SHELL", "pg_isready -U kwild"]
                    interval: 10s
                    timeout: 5s
                    retries: 12

                tn-node:
                  image: ghcr.io/trufnetwork/node:latest
                  container_name: tn-node
                  environment:
                    - SETUP_CHAIN_ID=${CHAIN_ID:-tn-v2.1}
                    - SETUP_DB_OWNER=${DB_OWNER:-postgres://kwild:kwild@kwil-postgres:5432/kwild}
                    - CONFIG_PATH=/root/.kwild
                  volumes:
                    - node_data:/root/.kwild
                    - /opt/tn/configs:/opt/configs:ro
                  ports:
                    - "50051:50051"
                    - "50151:50151"
                    - "8080:8080"
                    - "8484:8484"
                    - "26656:26656"
                    - "26657:26657"
                  depends_on:
                    kwil-postgres:
                      condition: service_healthy
                  networks:
                    - tn-network
                  restart: unless-stopped

                postgres-mcp:
                  image: crystaldba/postgres-mcp:latest
                  container_name: tn-mcp
                  environment:
                    - DATABASE_URI=postgresql://kwild:kwild@kwil-postgres:5432/kwild
                    - MCP_ACCESS_MODE=restricted
                    - MCP_TRANSPORT=sse
                  ports:
                    - "8000:8000"
                  depends_on:
                    - kwil-postgres
                    - tn-node
                  networks:
                    - tn-network
                  restart: unless-stopped
                  profiles:
                    - mcp

              volumes:
                postgres_data:
                node_data:

              networks:
                tn-network:
                  driver: bridge
              EOF
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
              ExecStart=/usr/bin/docker-compose up -d
              ExecStop=/usr/bin/docker-compose down
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

              # Default values
              NETWORK="testnet"
              PRIVATE_KEY=""
              ENABLE_MCP=false
              MCP_TRANSPORT="sse"
              MCP_ACCESS_MODE="restricted"

              # Parse command line arguments
              while [[ $# -gt 0 ]]; do
                case $1 in
                  --network)
                    NETWORK="$2"
                    shift 2
                    ;;
                  --private-key)
                    PRIVATE_KEY="$2"
                    shift 2
                    ;;
                  --enable-mcp)
                    ENABLE_MCP=true
                    shift
                    ;;
                  --mcp-transport)
                    MCP_TRANSPORT="$2"
                    shift 2
                    ;;
                  --mcp-access-mode)
                    MCP_ACCESS_MODE="$2"
                    shift 2
                    ;;
                  *)
                    echo "Unknown option $1"
                    exit 1
                    ;;
                esac
              done

              echo "Configuring TRUF.NETWORK node..."
              echo "Network: $NETWORK"
              echo "MCP enabled: $ENABLE_MCP"

              # Chain ID is always tn-v2.1 regardless of network
              CHAIN_ID="tn-v2.1"
              cd /opt/tn

              # Create .env file
              cat > .env << ENVEOF
              CHAIN_ID=$CHAIN_ID
              DB_OWNER=postgres://kwild:kwild@kwil-postgres:5432/kwild
              ENVEOF

              # Only set COMPOSE_PROFILES when MCP is enabled
              if [ "$ENABLE_MCP" = true ]; then
                echo "COMPOSE_PROFILES=mcp" >> .env
              fi

              # Handle private key if provided
              if [ -n "$PRIVATE_KEY" ]; then
                echo "Converting private key to nodekey.json..."
                # TODO: Implement private key to nodekey.json conversion
                echo "Private key conversion will be implemented"
              fi

              # Enable and start the service
              sudo systemctl daemon-reload
              sudo systemctl enable tn-node
              sudo systemctl start tn-node

              echo "TRUF.NETWORK node configuration complete!"
              echo "Service status: $(sudo systemctl is-active tn-node)"

              if [ "$ENABLE_MCP" = true ]; then
                # Get public IP with fallback to local IP
                PUBLIC_IP=$(curl -s --connect-timeout 5 ifconfig.co 2>/dev/null || curl -s --connect-timeout 5 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}' || echo "localhost")
                echo "MCP server will be available at: http://$PUBLIC_IP:8000/sse"
              fi
              EOF
            - sudo chmod +x /usr/local/bin/tn-node-configure

      - name: CreateUpdateScript
        action: ExecuteBash
        inputs:
          commands:
            - |
              sudo tee /usr/local/bin/update-node > /dev/null << 'EOF'
              #!/bin/bash
              set -e

              echo "Updating TRUF.NETWORK node to latest version..."
              cd /opt/tn

              # Pull latest images
              sudo -u tn docker-compose pull

              # Restart services with new images
              sudo -u tn docker-compose up -d

              echo "Node updated successfully!"
              echo "Service status: $(sudo systemctl is-active tn-node)"
              EOF
            - sudo chmod +x /usr/local/bin/update-node

      - name: EnableServices
        action: ExecuteBash
        inputs:
          commands:
            - sudo systemctl daemon-reload
            - sudo systemctl enable tn-node
`),
	})

	// Add explicit dependency
	infraConfig.AddDependency(instanceProfile)

	// Image recipe that combines both components
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
		},
		BlockDeviceMappings: &[]*awsimagebuilder.CfnImageRecipe_InstanceBlockDeviceMappingProperty{
			{
				DeviceName: jsii.String("/dev/sda1"),
				Ebs: &awsimagebuilder.CfnImageRecipe_EbsInstanceBlockDeviceSpecificationProperty{
					VolumeSize:          jsii.Number(20), // 20GB root volume
					VolumeType:          jsii.String("gp3"),
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
		RecipeArn:           *imageRecipe.AttrArn(),
		InfraConfigArn:      *infraConfig.AttrArn(),
		DistributionArn:     *distributionConfig.AttrArn(),
		S3BucketName:        *artifactsBucket.BucketName(),
		InstanceProfileArn:  *instanceProfile.AttrArn(),
	}

	return stack, exports
}
