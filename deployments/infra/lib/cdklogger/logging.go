package cdklogger

import (
	"fmt"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// LogInfo adds an INFO level message to the CDK construct's metadata.
// These messages are typically output during `cdk synth`.
func LogInfo(scope constructs.Construct, constructID string, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	finalMessage := message // Default to original message

	if constructID != "" {
		cdkPath := *scope.Node().Path() // Dereference to get string
		// Check if the cdkPath (e.g., "/Stack/Construct") ends with "/" + constructID (e.g., "/Construct")
		// Or if cdkPath (e.g., "/StackName") is "/" + constructID (e.g. "/StackName")
		// This avoids redundant prefixes like "[StackName] message" if path is already "/StackName"
		// or "[Construct] message" if path is "/Stack/Construct"
		if strings.HasSuffix(cdkPath, "/"+constructID) || cdkPath == "/"+constructID {
			// Prefix is redundant, use message as is (already set in finalMessage)
		} else {
			// Prefix is not redundant or provides more specific context
			finalMessage = fmt.Sprintf("[%s] %s", constructID, message)
		}
	}
	awscdk.Annotations_Of(scope).AddInfo(jsii.String(finalMessage))
}

// LogWarning adds a WARNING level message to the CDK construct's metadata.
func LogWarning(scope constructs.Construct, constructID string, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	finalMessage := message // Default to original message

	if constructID != "" {
		cdkPath := *scope.Node().Path() // Dereference to get string
		if strings.HasSuffix(cdkPath, "/"+constructID) || cdkPath == "/"+constructID {
			// Prefix is redundant
		} else {
			finalMessage = fmt.Sprintf("[%s] %s", constructID, message)
		}
	}
	awscdk.Annotations_Of(scope).AddWarning(jsii.String(finalMessage))
}

// LogError adds an ERROR level message to the CDK construct's metadata.
func LogError(scope constructs.Construct, constructID string, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	finalMessage := message // Default to original message

	if constructID != "" {
		cdkPath := *scope.Node().Path() // Dereference to get string
		if strings.HasSuffix(cdkPath, "/"+constructID) || cdkPath == "/"+constructID {
			// Prefix is redundant
		} else {
			finalMessage = fmt.Sprintf("[%s] %s", constructID, message)
		}
	}
	awscdk.Annotations_Of(scope).AddError(jsii.String(finalMessage))
}
