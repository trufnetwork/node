package config

import (
	"fmt"
	"strconv"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// Stack suffix is intended to be used after the stack name to differentiate between different stages.
func WithStackSuffix(scope constructs.Construct, stackName string) string {
	// Always append the standard suffix
	return stackName + "-Stack"
}

// DO NOT modify this function, change EC2 key pair name by 'cdk.json/context/keyPairName'.
func KeyPairName(scope constructs.Construct) string {
	keyPairName := "MyKeyPair"

	ctxValue := scope.Node().TryGetContext(jsii.String("keyPairName"))
	if v, ok := ctxValue.(string); ok {
		keyPairName = v
	}

	return keyPairName
}

func NumOfNodes(scope constructs.Construct) int {
	numOfNodes := 1

	ctxValue := scope.Node().TryGetContext(jsii.String("numOfNodes"))
	if ctxValue != nil {
		// ctxValue may be a float64 or a string
		switch v := ctxValue.(type) {
		case float64:
			numOfNodes = int(v)
		case string:
			var err error
			numOfNodes, err = strconv.Atoi(v)
			if err != nil {
				panic(fmt.Sprintf("numOfNodes context value is not a number: %s", v))
			}
		}
	}

	return numOfNodes
}
