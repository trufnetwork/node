// Package secrets names the SSM Parameter Store SecureStrings that hold the
// node keys and the gateway session secret, and grants instance roles read
// access to them.
//
// CloudFormation cannot create SecureString parameters, so they are created
// out of band with scripts/ssm-secrets.sh. Instances read them at boot, and
// their values never appear in a template, in user data or in a CDK asset.
package secrets

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

var (
	prefixPattern    = regexp.MustCompile(`^(/[A-Za-z0-9_.-]+)+$`)
	kmsKeyArnPattern = regexp.MustCompile(`^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]+$`)
)

// Store says where the SecureString parameters live.
type Store struct {
	// Prefix is the SSM path prefix, for example /tsn/prod.
	Prefix string
	// KmsKeyArn is the customer managed KMS key that encrypts the parameters.
	// Leave it empty for the AWS managed aws/ssm key.
	KmsKeyArn string
}

// DefaultPrefix is /tsn/<stage>, or /tsn/<stage>/<devPrefix> when a dev prefix
// is set. scripts/ssm-secrets.sh computes the same default.
func DefaultPrefix(stage, devPrefix string) string {
	if devPrefix == "" {
		return "/tsn/" + stage
	}
	return "/tsn/" + stage + "/" + devPrefix
}

// ValidatePrefix checks that prefix is an absolute SSM path without a
// trailing slash and outside the reserved /aws and /ssm trees.
func ValidatePrefix(prefix string) error {
	if !prefixPattern.MatchString(prefix) {
		return fmt.Errorf("SSM prefix %q must look like /a/b, using letters, digits, '.', '_' and '-'", prefix)
	}
	lower := strings.ToLower(prefix)
	for _, reserved := range []string{"/aws", "/ssm"} {
		if lower == reserved || strings.HasPrefix(lower, reserved+"/") {
			return fmt.Errorf("SSM prefix %q is under the reserved %s path", prefix, reserved)
		}
	}
	return nil
}

// ValidateKmsKeyArn checks that arn is a KMS key ARN (not an alias ARN).
func ValidateKmsKeyArn(arn string) error {
	if !kmsKeyArnPattern.MatchString(arn) {
		return fmt.Errorf("%q is not a KMS key ARN (arn:aws:kms:<region>:<account>:key/<id>)", arn)
	}
	return nil
}

// NodeKeyParameter is the parameter holding the nodekey.json of the node with
// the given 0-based index (index 0 is host node-1).
func (s Store) NodeKeyParameter(index int) string {
	return fmt.Sprintf("%s/node/%d/nodekey", s.Prefix, index)
}

// SessionSecretParameter is the parameter holding the gateway session secret.
func (s Store) SessionSecretParameter() string {
	return s.Prefix + "/kgw/session_secret"
}

// ParameterArn returns the ARN of the parameter name in the stack of scope.
func ParameterArn(scope constructs.IConstruct, name string) *string {
	return awscdk.Stack_Of(scope).FormatArn(&awscdk.ArnComponents{
		Service:      jsii.String("ssm"),
		Resource:     jsii.String("parameter"),
		ResourceName: jsii.String(strings.TrimPrefix(name, "/")),
		ArnFormat:    awscdk.ArnFormat_SLASH_RESOURCE_NAME,
	})
}

// GrantRead lets role read the parameter name and nothing else in SSM. With a
// customer managed key it also allows kms:Decrypt on that key through SSM.
func (s Store) GrantRead(role awsiam.IRole, name string) {
	role.AddToPrincipalPolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Effect:    awsiam.Effect_ALLOW,
		Actions:   jsii.Strings("ssm:GetParameter"),
		Resources: jsii.Strings(*ParameterArn(role, name)),
	}))
	if s.KmsKeyArn == "" {
		return
	}
	stack := awscdk.Stack_Of(role)
	role.AddToPrincipalPolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Effect:    awsiam.Effect_ALLOW,
		Actions:   jsii.Strings("kms:Decrypt"),
		Resources: jsii.Strings(s.KmsKeyArn),
		Conditions: &map[string]interface{}{
			"StringEquals": map[string]interface{}{
				"kms:ViaService": fmt.Sprintf("ssm.%s.%s", *stack.Region(), *stack.UrlSuffix()),
			},
		},
	}))
}
