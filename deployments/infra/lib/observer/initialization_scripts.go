package observer

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type ObserverScriptInput struct {
	ZippedAssetsDir string
	Params          *ObserverParameters
	Prefix          string
}

// - extract the zip with the compose files
//   - deployments/observer/observer-compose.yml
//   - deployments/observer/vector-prod-destination.yml
//   - deployments/observer/vector-sources.yml
// - create the systemd service
// - start the service
// - return the script

// # notes
// - has no header. It's supposed to be included in another initialization script
func GetObserverScript(input ObserverScriptInput) *string {
	observerDir := "/home/ec2-user/observer"
	startScriptPath := "/usr/local/bin/start-observer.sh"
	script := utils.UnzipFileScript(input.ZippedAssetsDir, observerDir)
	startObserverScriptContent, err := CreateStartObserverScript(CreateStartObserverScriptInput{
		Params:          input.Params,
		Prefix:          input.Prefix,
		ObserverDir:     observerDir,
		StartScriptPath: startScriptPath,
	})
	if err != nil {
		panic(err)
	}
	script += *awscdk.Fn_Sub(jsii.String(startObserverScriptContent), nil) + "\n"
	script += utils.CreateSystemdServiceScript(
		"observer",
		"Observer Compose",
		startScriptPath,
		fmt.Sprintf("/bin/bash -c \"docker compose -f %s/observer-compose.yml down\"", observerDir),
		nil,
	)
	return jsii.String(script)
}
