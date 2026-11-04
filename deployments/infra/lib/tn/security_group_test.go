package tn_test

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/tn"
)

// rule is one ingress rule of the TN security group.
type rule struct {
	Port        float64
	CIDR        string
	Description string
}

// The ports that stay open to any address whatever the RPC sources are.
var openPorts = []rule{
	{1337, "0.0.0.0/0", "Allow requests to the TN Indexer port."},
	{26656, "0.0.0.0/0", "Allow requests to the TN P2P port."},
	{26657, "0.0.0.0/0", "Allow requests to the TN Comet BFT RPC port."},
	{22, "0.0.0.0/0", "Allow requests to the SSH port."},
}

// synthIngress builds the TN security group the way the ValidatorSet does,
// with the given CDK context, and returns its ingress rules in template order.
func synthIngress(t *testing.T, context map[string]interface{}) []rule {
	t.Helper()
	app := awscdk.NewApp(&awscdk.AppProps{Context: &context})
	stack := awscdk.NewStack(app, jsii.String("TestStack"), &awscdk.StackProps{
		Env: &awscdk.Environment{Account: jsii.String("123456789012"), Region: jsii.String("us-east-1")},
	})
	vpc := awsec2.NewVpc(stack, jsii.String("Vpc"), &awsec2.VpcProps{NatGateways: jsii.Number(0)})
	tn.NewTNSecurityGroup(constructs.NewConstruct(stack, jsii.String("ValidatorSet")), tn.NewTNSecurityGroupInput{Vpc: vpc})

	groups := *assertions.Template_FromStack(stack, nil).FindResources(jsii.String("AWS::EC2::SecurityGroup"), nil)
	// The deployed group has this logical id; keeping it (and the description)
	// lets CloudFormation update the group in place rather than replace it.
	group, ok := groups["ValidatorSetTNDBSGA544AF6F"]
	if !ok || len(groups) != 1 {
		t.Fatalf("want the one group ValidatorSetTNDBSGA544AF6F, got %v", groups)
	}
	props := (*group)["Properties"].(map[string]interface{})
	if got := props["GroupDescription"]; got != "TN-DB Instance security group." {
		t.Fatalf("group description changed to %v", got)
	}

	var rules []rule
	ingress, _ := props["SecurityGroupIngress"].([]interface{})
	for _, raw := range ingress {
		r := raw.(map[string]interface{})
		if r["IpProtocol"] != "tcp" || r["FromPort"] != r["ToPort"] {
			t.Fatalf("want single tcp ports, got %v", r)
		}
		rules = append(rules, rule{r["FromPort"].(float64), r["CidrIp"].(string), r["Description"].(string)})
	}
	return rules
}

func TestRPCPortOpenToAnyAddressWithoutSources(t *testing.T) {
	want := append([]rule{{8484, "0.0.0.0/0", "Allow requests to the TN RPC port."}}, openPorts...)
	for name, context := range map[string]map[string]interface{}{
		"no rpcSources": {"stackSuffix": "dev"},
		"suffix not listed": {
			"stackSuffix": "dev",
			"rpcSources":  map[string]interface{}{"prod": map[string]interface{}{"203.0.113.10/32": "gateway"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := synthIngress(t, context); !reflect.DeepEqual(got, want) {
				t.Fatalf("ingress rules\n got: %v\nwant: %v", got, want)
			}
		})
	}
}

func TestRPCPortOpenOnlyToListedSources(t *testing.T) {
	got := synthIngress(t, map[string]interface{}{
		"stackSuffix": "prod",
		"rpcSources": map[string]interface{}{
			"prod": map[string]interface{}{
				"203.0.113.10/32": "gateway",
				"198.51.100.7/32": "indexer",
			},
			"testnet": map[string]interface{}{"192.0.2.1/32": "testnet gateway"},
		},
	})
	want := append([]rule{
		{8484, "198.51.100.7/32", "Allow requests to the TN RPC port from the indexer."},
		{8484, "203.0.113.10/32", "Allow requests to the TN RPC port from the gateway."},
	}, openPorts...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ingress rules\n got: %v\nwant: %v", got, want)
	}
}

func TestRPCPortClosedWhenNoSourcesListed(t *testing.T) {
	got := synthIngress(t, map[string]interface{}{
		"stackSuffix": "prod",
		"rpcSources":  map[string]interface{}{"prod": map[string]interface{}{}},
	})
	if !reflect.DeepEqual(got, openPorts) {
		t.Fatalf("ingress rules\n got: %v\nwant: %v", got, openPorts)
	}
}

// The prod stack reads its RPC sources from cdk.json, so a redeploy keeps the
// port limited to them.
func TestProdRPCPortLimitedByCdkJSON(t *testing.T) {
	raw, err := os.ReadFile("../../cdk.json")
	if err != nil {
		t.Fatal(err)
	}
	var cdkJSON struct {
		Context map[string]interface{} `json:"context"`
	}
	if err := json.Unmarshal(raw, &cdkJSON); err != nil {
		t.Fatal(err)
	}
	cdkJSON.Context["stackSuffix"] = "prod"

	got := synthIngress(t, cdkJSON.Context)
	n := len(got) - len(openPorts)
	if n <= 0 || !reflect.DeepEqual(got[n:], openPorts) {
		t.Fatalf("want RPC rules followed by the open ports, got %v", got)
	}
	for _, r := range got[:n] {
		if r.Port != 8484 || r.CIDR == "0.0.0.0/0" {
			t.Fatalf("want the RPC port open only to listed sources, got %v", r)
		}
	}
}
