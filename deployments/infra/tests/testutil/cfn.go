package testutil

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/jsii-runtime-go"
)

// FlattenCfn renders a CloudFormation value as text, so tests can search user
// data and ARNs: Fn::Join is joined, Fn::Base64 unwrapped, Ref and Fn::GetAtt
// become ${...}, and anything else is written as JSON.
func FlattenCfn(v interface{}) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case []interface{}:
		parts := make([]string, len(val))
		for i, p := range val {
			parts[i] = FlattenCfn(p)
		}
		return strings.Join(parts, ",")
	case map[string]interface{}:
		if inner, ok := val["Fn::Base64"]; ok {
			return FlattenCfn(inner)
		}
		if join, ok := val["Fn::Join"].([]interface{}); ok && len(join) == 2 {
			sep, _ := join[0].(string)
			items, _ := join[1].([]interface{})
			parts := make([]string, len(items))
			for i, p := range items {
				parts[i] = FlattenCfn(p)
			}
			return strings.Join(parts, sep)
		}
		if ref, ok := val["Ref"].(string); ok {
			return "${" + ref + "}"
		}
		if att, ok := val["Fn::GetAtt"].([]interface{}); ok {
			names := make([]string, len(att))
			for i, a := range att {
				names[i] = fmt.Sprint(a)
			}
			return "${" + strings.Join(names, ".") + "}"
		}
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(raw)
}

// Resources returns the resources of the given type, keyed by logical ID.
func Resources(template assertions.Template, resourceType string) map[string]map[string]interface{} {
	found := template.FindResources(jsii.String(resourceType), nil)
	out := make(map[string]map[string]interface{}, len(*found))
	for id, res := range *found {
		if res != nil {
			out[id] = *res
		}
	}
	return out
}

// LogicalIDsContaining returns the sorted logical IDs that contain part.
func LogicalIDsContaining(resources map[string]map[string]interface{}, part string) []string {
	var ids []string
	for id := range resources {
		if strings.Contains(id, part) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// LaunchTemplateUserData returns the user data of a launch template as text.
func LaunchTemplateUserData(resource map[string]interface{}) string {
	props, _ := resource["Properties"].(map[string]interface{})
	data, _ := props["LaunchTemplateData"].(map[string]interface{})
	return FlattenCfn(data["UserData"])
}

// PolicyGrant is one statement of an IAM policy attached to a role.
type PolicyGrant struct {
	Role      string   // logical ID of the role
	Effect    string   // Allow or Deny
	Actions   []string // actions as written
	Resources []string // resources flattened to text
}

// PolicyGrants lists the statements of every AWS::IAM::Policy per attached role.
func PolicyGrants(template assertions.Template) []PolicyGrant {
	var grants []PolicyGrant
	for _, policy := range Resources(template, "AWS::IAM::Policy") {
		props, _ := policy["Properties"].(map[string]interface{})
		doc, _ := props["PolicyDocument"].(map[string]interface{})
		roles, _ := props["Roles"].([]interface{})
		statements, _ := doc["Statement"].([]interface{})
		for _, raw := range statements {
			stmt, _ := raw.(map[string]interface{})
			actions := asStrings(stmt["Action"])
			resources := []string{}
			switch r := stmt["Resource"].(type) {
			case []interface{}:
				for _, item := range r {
					resources = append(resources, FlattenCfn(item))
				}
			default:
				resources = append(resources, FlattenCfn(r))
			}
			for _, role := range roles {
				ref, _ := role.(map[string]interface{})["Ref"].(string)
				effect, _ := stmt["Effect"].(string)
				grants = append(grants, PolicyGrant{Role: ref, Effect: effect, Actions: actions, Resources: resources})
			}
		}
	}
	return grants
}

func asStrings(v interface{}) []string {
	switch val := v.(type) {
	case string:
		return []string{val}
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			out = append(out, fmt.Sprint(item))
		}
		return out
	}
	return nil
}
