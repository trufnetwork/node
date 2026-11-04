package config

import (
	"fmt"
	"sort"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// ContextRPCSources is the CDK context key that lists, per stack suffix, the
// sources allowed to reach the TN RPC port. Each source is an IPv4 CIDR mapped
// to the name of the host behind it:
//
//	"rpcSources": {"prod": {"203.0.113.10/32": "mainnet gateway"}}
//
// A stack whose suffix has no entry keeps the port open to any address.
const ContextRPCSources = "rpcSources"

// RPCSource is an address range allowed to reach the TN RPC port.
type RPCSource struct {
	CIDR string // IPv4 CIDR, e.g. "203.0.113.10/32"
	Name string // host behind the CIDR, used in the rule description
}

// RPCSources returns the sources allowed to reach the TN RPC port for the
// current stack suffix, sorted by CIDR. ok is false when the stack suffix has
// no entry under ContextRPCSources; an entry with no sources is ok and empty.
func RPCSources(scope constructs.Construct) (sources []RPCSource, ok bool) {
	raw := scope.Node().TryGetContext(jsii.String(ContextRPCSources))
	if raw == nil {
		return nil, false
	}
	byStackSuffix, isMap := raw.(map[string]interface{})
	if !isMap {
		panic(fmt.Sprintf("Context variable '%s' must map stack suffixes to sources, got %T", ContextRPCSources, raw))
	}

	stackSuffix := StackSuffix(scope)
	entry, found := byStackSuffix[stackSuffix]
	if !found {
		return nil, false
	}
	byCIDR, isMap := entry.(map[string]interface{})
	if !isMap {
		panic(fmt.Sprintf("Context variable '%s.%s' must map CIDRs to host names, got %T", ContextRPCSources, stackSuffix, entry))
	}

	sources = make([]RPCSource, 0, len(byCIDR))
	for cidr, name := range byCIDR {
		nameStr, isString := name.(string)
		if !isString || nameStr == "" {
			panic(fmt.Sprintf("Context variable '%s.%s' needs a host name for %s", ContextRPCSources, stackSuffix, cidr))
		}
		sources = append(sources, RPCSource{CIDR: cidr, Name: nameStr})
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].CIDR < sources[j].CIDR })
	return sources, true
}
