package observer

import (
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/utils"
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
	script := utils.UnzipFileScript(input.ZippedAssetsDir, "/home/ec2-user/observer")
	script += CreateStartObserverScript(input.Params, input.Prefix)
	script += utils.CreateSystemdServiceScript(
		"observer",
		"Observer Compose",
		"/bin/bash /usr/local/bin/start-observer.sh",
		"/bin/bash -c \"docker compose -f /home/ec2-user/observer/observer-compose.yml down\"",
		nil,
	)
	return jsii.String(script)
}
