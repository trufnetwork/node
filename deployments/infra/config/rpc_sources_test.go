package config_test

import (
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
