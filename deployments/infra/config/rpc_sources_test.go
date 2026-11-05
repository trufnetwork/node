package config_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/trufnetwork/node/infra/config"
)

// A malformed entry stops the synth instead of leaving the RPC port open.
func TestRPCSourcesRejectsMalformedContext(t *testing.T) {
	for name, value := range map[string]interface{}{
		"flat list instead of a map": "203.0.113.10/32,198.51.100.7/32",
		"list of CIDRs per suffix":   map[string]interface{}{"prod": []interface{}{"203.0.113.10/32"}},
		"CIDR without a host name":   map[string]interface{}{"prod": map[string]interface{}{"203.0.113.10/32": ""}},
		"host name EC2 rejects":      map[string]interface{}{"prod": map[string]interface{}{"203.0.113.10/32": "ops team's box"}},
		"host name too long":         map[string]interface{}{"prod": map[string]interface{}{"203.0.113.10/32": strings.Repeat("a", 201)}},
	} {
		t.Run(name, func(t *testing.T) {
			app := awscdk.NewApp(&awscdk.AppProps{Context: &map[string]interface{}{
				"stackSuffix":            "prod",
				config.ContextRPCSources: value,
			}})
			defer func() {
				if recover() == nil {
					t.Fatalf("want a panic for %v", value)
				}
			}()
			config.RPCSources(app)
		})
	}
}

// Every character EC2 accepts in a rule description is a valid host name.
func TestRPCSourcesAcceptsEC2DescriptionCharacters(t *testing.T) {
	name := "a-z A-Z 0-9 ._-:/()#,@[]+=&;{}!$*"
	app := awscdk.NewApp(&awscdk.AppProps{Context: &map[string]interface{}{
		"stackSuffix":            "prod",
		config.ContextRPCSources: map[string]interface{}{"prod": map[string]interface{}{"203.0.113.10/32": name}},
	}})
	want := []config.RPCSource{{CIDR: "203.0.113.10/32", Name: name}}
	if got, ok := config.RPCSources(app); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, %v; want %v, true", got, ok, want)
	}
}

// On the prod stage a stack suffix without an entry stops the synth instead of
// leaving the RPC port open. CDK drops JSON null values from context, so a null
// entry or a null rpcSources reads as missing and stops it too.
func TestRPCSourcesRequiredOnProdStage(t *testing.T) {
	for name, context := range map[string]map[string]interface{}{
		"no rpcSources":   {"stage": "prod", "stackSuffix": "prod"},
		"null rpcSources": {"stage": "prod", "stackSuffix": "prod", config.ContextRPCSources: nil},
		"suffix not listed": {"stage": "prod", "stackSuffix": "prod",
			config.ContextRPCSources: map[string]interface{}{"testnet": map[string]interface{}{"192.0.2.1/32": "testnet gateway"}}},
		"null entry": {"stage": "prod", "stackSuffix": "prod",
			config.ContextRPCSources: map[string]interface{}{"prod": nil}},
	} {
		t.Run(name, func(t *testing.T) {
			app := awscdk.NewApp(&awscdk.AppProps{Context: &context})
			defer func() {
				if recover() == nil {
					t.Fatalf("want a panic for %v", context)
				}
			}()
			config.RPCSources(app)
		})
	}
}

// Off the prod stage a stack suffix without an entry keeps the port open.
func TestRPCSourcesOptionalOffProdStage(t *testing.T) {
	app := awscdk.NewApp(&awscdk.AppProps{Context: &map[string]interface{}{
		"stage":                  "dev",
		"stackSuffix":            "dev",
		config.ContextRPCSources: map[string]interface{}{"dev": nil, "prod": map[string]interface{}{"203.0.113.10/32": "gateway"}},
	}})
	if sources, ok := config.RPCSources(app); ok || sources != nil {
		t.Fatalf("got %v, %v; want no sources", sources, ok)
	}
}
