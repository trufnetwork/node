package config

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	domaincfg "github.com/trufnetwork/node/infra/config/domain"
)

// ContextRPCSources is the CDK context key that lists, per stack suffix, the
// sources allowed to reach the TN RPC port. Each source is an IPv4 CIDR mapped
// to the name of the host behind it:
//
//	"rpcSources": {"prod": {"203.0.113.10/32": "mainnet gateway"}}
//
// A stack whose suffix has no entry keeps the port open to any address, except
// on the prod stage, where a missing entry stops the synth. CDK drops JSON null
// values from context, so a null entry counts as missing.
const ContextRPCSources = "rpcSources"

// hostNameRegex matches the characters EC2 accepts in a rule description, which
// carries the host name. The 200-character cap leaves room for the rest of the
// description within EC2's limit of 255.
var hostNameRegex = regexp.MustCompile(`^[a-zA-Z0-9 ._:/()#,@\[\]+=&;{}!$*-]{1,200}$`)

// RPCSource is an address range allowed to reach the TN RPC port.
type RPCSource struct {
	CIDR string // IPv4 CIDR, e.g. "203.0.113.10/32"
	Name string // host behind the CIDR, used in the rule description
}

// RPCSources returns the sources allowed to reach the TN RPC port for the
// current stack suffix, sorted by CIDR. ok is false when the stack suffix has
// no entry under ContextRPCSources; an entry with no sources is ok and empty.
// It panics when the stage is prod and the stack suffix has no entry.
func RPCSources(scope constructs.Construct) (sources []RPCSource, ok bool) {
	raw := scope.Node().TryGetContext(jsii.String(ContextRPCSources))
	byStackSuffix, isMap := raw.(map[string]interface{})
	if raw != nil && !isMap {
		panic(fmt.Sprintf("Context variable '%s' must map stack suffixes to sources, got %T", ContextRPCSources, raw))
	}

	stackSuffix := StackSuffix(scope)
	entry, found := byStackSuffix[stackSuffix]
	if !found {
		// A null entry reaches here too, so the prod stage stops rather than
		// leave the port open to any address.
		if stage, _ := scope.Node().TryGetContext(jsii.String(ContextStage)).(string); domaincfg.StageType(stage) == domaincfg.StageProd {
			panic(fmt.Sprintf("Context variable '%s.%s' must list the RPC sources on the '%s' stage; use {} to close the port", ContextRPCSources, stackSuffix, stage))
		}
		return nil, false
	}
	byCIDR, isMap := entry.(map[string]interface{})
	if !isMap {
		panic(fmt.Sprintf("Context variable '%s.%s' must map CIDRs to host names, got %T", ContextRPCSources, stackSuffix, entry))
	}

	sources = make([]RPCSource, 0, len(byCIDR))
	for cidr, name := range byCIDR {
		nameStr, isString := name.(string)
		if !isString || !hostNameRegex.MatchString(nameStr) {
			panic(fmt.Sprintf("Context variable '%s.%s' needs a host name for %s of up to 200 letters, digits, spaces and ._-:/()#,@[]+=&;{}!$*",
				ContextRPCSources, stackSuffix, cidr))
		}
		sources = append(sources, RPCSource{CIDR: cidr, Name: nameStr})
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].CIDR < sources[j].CIDR })
	return sources, true
}
