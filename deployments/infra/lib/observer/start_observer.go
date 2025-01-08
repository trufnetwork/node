package observer

import (
	"fmt"
	"path"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type CreateStartObserverScriptInput struct {
	Params          *ObserverParameters
	Prefix          string
	ObserverDir     string
	StartScriptPath string
}

// CreateStartObserverScript creates the script that starts the observer
// - fetches the parameters from SSM
// - writes the parameters to the .env file
// - starts the observer
// Start of Selection
func CreateStartObserverScript(input CreateStartObserverScriptInput) string {
	descriptors, err := utils.GetParameterDescriptors(input.Params)
	if err != nil {
		// Handle error appropriately
		return ""
	}

	var sb strings.Builder

	sb.WriteString(`#!/bin/bash

set -x

AWS_REGION=` + *awscdk.Aws_REGION() + `

# Parameterized paths
OBSERVER_DIR="` + input.ObserverDir + `"
ENV_FILE="$OBSERVER_DIR/.env"
COMPOSE_FILE="$OBSERVER_DIR/observer-compose.yml"
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
			ssmPath := path.Join(input.Prefix, desc.SSMPath)
			isSecure := "false"
			if desc.IsSecure {
				isSecure = "true"
			}
			sb.WriteString(fmt.Sprintf(`fetch_parameter "%s" "%s" "%s"
`, ssmPath, isSecure, desc.EnvName))
		} else {
			// Handle non-SSM parameters
			sb.WriteString(fmt.Sprintf(`%s='%s'
`, desc.EnvName, desc.EnvValue))
		}
	}

	sb.WriteString(`
# Write environment variables to .env file
cat << EOF1 > $ENV_FILE
`)

	for _, desc := range descriptors {
		sb.WriteString(fmt.Sprintf(`%s=${%s}
`, desc.EnvName, desc.EnvName))
	}

	sb.WriteString(`EOF1

chmod 600 $ENV_FILE
chown ec2-user:ec2-user $ENV_FILE

# Start Docker Compose
docker compose -f $COMPOSE_FILE up -d --wait || true
`)

	scriptContent := sb.String()
	initScript := `
START_SCRIPT="` + input.StartScriptPath + `"

cat <<'EOF2' > $START_SCRIPT
` + scriptContent + `
EOF2

chmod +x $START_SCRIPT
`

	return initScript
}
