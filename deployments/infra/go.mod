module github.com/trufnetwork/node/infra

go 1.22.7

toolchain go1.23.6

require (
	github.com/aws/aws-cdk-go/awscdk/v2 v2.146.0
	github.com/aws/aws-cdk-go/awscdklambdagoalpha/v2 v2.146.0-alpha.0
	github.com/aws/aws-lambda-go v1.47.0
	github.com/aws/aws-sdk-go v1.55.5
	github.com/aws/constructs-go/constructs/v10 v10.3.0
	github.com/aws/jsii-runtime-go v1.99.0
	github.com/caarlos0/env/v11 v11.3.1
	github.com/trufnetwork/node v1.2.0
	go.uber.org/zap v1.27.0
)

replace github.com/trufnetwork/node/infra => ./

replace github.com/trufnetwork/node => ../../

require (
	github.com/fbiville/markdown-table-formatter v0.3.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/exp v0.0.0-20250128182459-e0ece0dbea4c // indirect
	golang.org/x/sync v0.11.0 // indirect
)

require (
	github.com/Masterminds/semver/v3 v3.2.1 // indirect
	github.com/cdklabs/awscdk-asset-awscli-go/awscliv1/v2 v2.2.202 // indirect
	github.com/cdklabs/awscdk-asset-kubectl-go/kubectlv20/v2 v2.1.2 // indirect
	github.com/cdklabs/awscdk-asset-node-proxy-agent-go/nodeproxyagentv6/v2 v2.0.3 // indirect
	github.com/fatih/color v1.17.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.6 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.22.0
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/yuin/goldmark v1.4.13 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/tools v0.29.0 // indirect
)
