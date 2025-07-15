package validation

import (
	"fmt"

	"github.com/trufnetwork/node/extensions/tn_cache/config/sources"
)

// Rule represents a validation rule that can be applied to a StreamSpec
type Rule interface {
	Name() string
	Validate(spec sources.StreamSpec) error
}

// RuleSet holds a collection of validation rules and applies them sequentially
type RuleSet struct {
	rules []Rule
}

// NewRuleSet creates a new validation rule set
func NewRuleSet(rules ...Rule) *RuleSet {
	return &RuleSet{rules: rules}
}

// DefaultRules returns the standard set of validation rules for stream configurations
func DefaultRules() *RuleSet {
	return NewRuleSet(
		&EthereumAddressRule{},
		&StreamIDRule{},
		&CronScheduleRule{},
		&TimeRangeRule{},
	)
}

// Validate applies all rules to a single StreamSpec
func (rs *RuleSet) Validate(spec sources.StreamSpec) error {
	for _, rule := range rs.rules {
		if err := rule.Validate(spec); err != nil {
			return fmt.Errorf("%s validation failed: %w", rule.Name(), err)
		}
	}
	return nil
}

// ValidateAll applies all rules to a slice of StreamSpecs
func (rs *RuleSet) ValidateAll(specs []sources.StreamSpec) error {
	for i, spec := range specs {
		if err := rs.Validate(spec); err != nil {
			return fmt.Errorf("spec %d: %w", i, err)
		}
	}
	return nil
}

// AddRule adds a new rule to the rule set
func (rs *RuleSet) AddRule(rule Rule) {
	rs.rules = append(rs.rules, rule)
}

// Rules returns a copy of all rules in the set
func (rs *RuleSet) Rules() []Rule {
	result := make([]Rule, len(rs.rules))
	copy(result, rs.rules)
	return result
}