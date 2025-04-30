// Package renderer loads embedded Bash templates under deployments/infra/scripts/templates/
// and renders them with sprig functions.
//
// It exists primarily so that raw user-data Bash scripts for EC2 instances
// live as separate, easily readable `.tmpl` files outside of Go string literals,
// improving maintainability and readability for infrastructure setup scripts.
//
// Example:
//
//	import (
//	    "github.com/trufnetwork/node/infra/scripts/renderer"
//	    "github.com/trufnetwork/node/infra/lib/utils" // For input struct
//	)
//
//	func configureDocker() (string, error) {
//	    input := utils.ConfigureDockerInput{ DataRoot: jsii.String("/mnt/docker") }
//	    script, err := renderer.Render(renderer.TplConfigureDocker, input)
//	    if err != nil { return "", err }
//	    return script, nil
//	}
package renderer
