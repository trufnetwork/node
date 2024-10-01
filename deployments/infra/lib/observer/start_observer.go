package observer

import (
	"fmt"
	"path"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

// CreateStartObserverScript creates the script that starts the observer
// - fetches the parameters from SSM
// - writes the parameters to the .env file
// - starts the observer
func CreateStartObserverScript(params *ObserverParameters, prefix string) string {
	descriptors, err := utils.GetParameterDescriptors(params)
	if err != nil {
		// Handle error appropriately
		return ""
	}

	var sb strings.Builder

	sb.WriteString(`#!/bin/bash
set -e

AWS_REGION=` + *awscdk.Aws_REGION() + `

fetch_parameter() {
    local param_name="$1"
    local is_secure="$2"
    local env_var_name="$3"

    if [ "$is_secure" = "true" ]; then
        value=$(aws ssm get-parameter --name "$param_name" --with-decryption --query "Parameter.Value" --output text --region $AWS_REGION)
    else
        value=$(aws ssm get-parameter --name "$param_name" --query "Parameter.Value" --output text --region $AWS_REGION)
    fi

    if [ -z "$value" ]; then
        echo "Error: Parameter $param_name not found or empty"
        exit 1
    fi

    export "$env_var_name=$value"
}

# Fetch parameters
`)

	for _, desc := range descriptors {
		if desc.IsSSMParameter {
			ssmPath := path.Join(prefix, desc.SSMPath)
			isSecure := "false"
			if desc.IsSecure {
				isSecure = "true"
			}
			sb.WriteString(fmt.Sprintf(`fetch_parameter "%s" "%s" "%s"
`, ssmPath, isSecure, desc.EnvName))
		} else {
			// Handle non-SSM parameters
			sb.WriteString(fmt.Sprintf(`%s=%s
`, desc.EnvName, desc.EnvValue))
		}
	}

	sb.WriteString(`
# Write environment variables to .env file
cat << EOF1 > /home/ec2-user/observer/.env
`)

	for _, desc := range descriptors {
		sb.WriteString(fmt.Sprintf(`%s=${%s}
`, desc.EnvName, desc.EnvName))
	}

	sb.WriteString(`EOF1

chmod 600 /home/ec2-user/observer/.env
chown ec2-user:ec2-user /home/ec2-user/observer/.env

# Start Docker Compose
docker compose -f /home/ec2-user/observer/observer-compose.yml up -d --wait || true
`)

	// Write the script to /usr/local/bin/start-observer.sh
	scriptContent := sb.String()
	initScript := `
cat <<'EOF2' > /usr/local/bin/start-observer.sh
` + scriptContent + `
EOF2

chmod +x /usr/local/bin/start-observer.sh
`

	return initScript
}
