package secrets_test

import (
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/jsii-runtime-go"
	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/node/infra/lib/secrets"
	"github.com/trufnetwork/node/infra/tests/testutil"
)

func TestParameterNames(t *testing.T) {
	require.Equal(t, "/tsn/prod", secrets.DefaultPrefix("prod", ""))
	require.Equal(t, "/tsn/dev/alice", secrets.DefaultPrefix("dev", "alice"))

	store := secrets.Store{Prefix: "/tsn/prod"}
	require.Equal(t, "/tsn/prod/node/0/nodekey", store.NodeKeyParameter(0))
	require.Equal(t, "/tsn/prod/node/3/nodekey", store.NodeKeyParameter(3))
	require.Equal(t, "/tsn/prod/kgw/session_secret", store.SessionSecretParameter())
}

func TestValidatePrefix(t *testing.T) {
	for _, ok := range []string{"/tsn/prod", "/tsn/dev/alice-1", "/a", "/team.x/y_z"} {
		require.NoError(t, secrets.ValidatePrefix(ok), ok)
	}
	for _, bad := range []string{"", "tsn/prod", "/tsn/prod/", "//tsn", "/tsn prod", "/aws/x", "/SSM", "/tsn/$x"} {
		require.Error(t, secrets.ValidatePrefix(bad), bad)
	}
}

func TestValidateKmsKeyArn(t *testing.T) {
	require.NoError(t, secrets.ValidateKmsKeyArn("arn:aws:kms:us-east-2:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"))
	require.NoError(t, secrets.ValidateKmsKeyArn("arn:aws:kms:us-east-2:123456789012:key/mrk-1234abcd12ab34cd56ef1234567890ab"))
	require.Error(t, secrets.ValidateKmsKeyArn("arn:aws:kms:us-east-2:123456789012:alias/aws/ssm"))
	require.Error(t, secrets.ValidateKmsKeyArn("1234abcd-12ab-34cd-56ef-1234567890ab"))
}

func TestGrantRead(t *testing.T) {
	const keyArn = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"

	for _, tc := range []struct {
		name    string
		store   secrets.Store
		withKms bool
	}{
		{name: "aws managed key", store: secrets.Store{Prefix: "/tsn/test"}},
		{name: "customer managed key", store: secrets.Store{Prefix: "/tsn/test", KmsKeyArn: keyArn}, withKms: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := awscdk.NewApp(nil)
			stack := awscdk.NewStack(app, jsii.String("TestStack"), &awscdk.StackProps{
				Env: &awscdk.Environment{Account: jsii.String("123456789012"), Region: jsii.String("us-east-1")},
			})
			role := awsiam.NewRole(stack, jsii.String("Role"), &awsiam.RoleProps{
				AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
			})
			tc.store.GrantRead(role, tc.store.NodeKeyParameter(1))

			template := assertions.Template_FromStack(stack, nil)
			grants := testutil.PolicyGrants(template)

			want := []testutil.PolicyGrant{{
				Effect:    "Allow",
				Actions:   []string{"ssm:GetParameter"},
				Resources: []string{"arn:${AWS::Partition}:ssm:us-east-1:123456789012:parameter/tsn/test/node/1/nodekey"},
			}}
			if tc.withKms {
				want = append(want, testutil.PolicyGrant{
					Effect:    "Allow",
					Actions:   []string{"kms:Decrypt"},
					Resources: []string{keyArn},
				})
				require.Equal(t, "ssm.us-east-1.${AWS::URLSuffix}", kmsViaService(t, template))
			}
			for i := range grants {
				grants[i].Role = ""
			}
			require.Equal(t, want, grants)
		})
	}
}

// kmsViaService returns the kms:ViaService condition of the kms:Decrypt
// statement, flattened to text.
func kmsViaService(t *testing.T, template assertions.Template) string {
	t.Helper()
	for _, policy := range testutil.Resources(template, "AWS::IAM::Policy") {
		props := policy["Properties"].(map[string]interface{})
		doc := props["PolicyDocument"].(map[string]interface{})
		for _, raw := range doc["Statement"].([]interface{}) {
			stmt := raw.(map[string]interface{})
			if stmt["Action"] != "kms:Decrypt" {
				continue
			}
			cond := stmt["Condition"].(map[string]interface{})["StringEquals"].(map[string]interface{})
			return testutil.FlattenCfn(cond["kms:ViaService"])
		}
	}
	t.Fatal("no kms:Decrypt statement")
	return ""
}
