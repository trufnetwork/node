package config

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
)

func IsStackInSynthesis(scope constructs.Construct) bool {
	stack := awscdk.Stack_Of(scope)

	// If the scope is not associated with a stack, return false
	if stack == nil {
		return false
	}

	return *stack.BundlingRequired()
}
