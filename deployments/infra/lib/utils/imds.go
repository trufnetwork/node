package utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/jsii-runtime-go"
)

// ImdsHopLimit keeps instance metadata, and so the instance role credentials,
// reachable from the host only. Containers on a bridge network are one hop
// further and get no answer. No container here needs instance credentials.
const ImdsHopLimit = 1

// WithImdsV2 sets the instance metadata options used by every launch template:
// IMDSv2 only, hop limit ImdsHopLimit.
func WithImdsV2(props *awsec2.LaunchTemplateProps) *awsec2.LaunchTemplateProps {
	props.RequireImdsv2 = jsii.Bool(true)
	props.HttpTokens = awsec2.LaunchTemplateHttpTokens_REQUIRED
	props.HttpEndpoint = jsii.Bool(true)
	props.HttpPutResponseHopLimit = jsii.Number(ImdsHopLimit)
	return props
}
