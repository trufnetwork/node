package observer

import (
	"fmt"

	"github.com/trufnetwork/node/infra/lib/utils"
	"github.com/trufnetwork/node/infra/scripts/renderer"
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
func CreateStartObserverScript(input CreateStartObserverScriptInput) (string, error) {
	descriptors, err := utils.GetParameterDescriptors(input.Params)
	if err != nil {
		// Return error instead of panicking
		return "", fmt.Errorf("get observer parameter descriptors: %w", err)
	}

	// Map utils.ParameterDescriptor to renderer.ParameterDescriptor
	rendererParams := make([]renderer.ParameterDescriptor, len(descriptors))
	for i, desc := range descriptors {
		rendererParams[i] = renderer.ParameterDescriptor{
			EnvName:        desc.EnvName,
			EnvValue:       desc.EnvValue,
			IsSSMParameter: desc.IsSSMParameter,
			SSMPath:        desc.SSMPath,
			IsSecure:       desc.IsSecure,
		}
	}

	// Data for the TplObserverStart template using type from renderer package
	tplData := renderer.ObserverStartData{
		ObserverDir: input.ObserverDir,
		Prefix:      input.Prefix,
		Params:      rendererParams, // Pass the mapped slice
	}

	// Render the main script body using the template
	body, err := renderer.Render(renderer.TplObserverStart, tplData)
	if err != nil {
		return "", fmt.Errorf("render %s: %w", renderer.TplObserverStart, err)
	}

	// Use the helper to wrap the script body
	return utils.WrapAsFile(body, input.StartScriptPath), nil
}
