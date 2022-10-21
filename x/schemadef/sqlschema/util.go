package sqlschema

import (
	"encoding/csv"
	"fmt"
	"path/filepath"
	"strings"
)

// DefaultValue returns the string represents the DEFAULT of a column.
func DefaultValue(c *Column) (string, bool) {
	switch x := c.Default.(type) {
	case nil:
		return "", false
	case *Literal:
		return x.V, true
	case *RawExpr:
		return x.X, true
	default:
		panic(fmt.Sprintf("unexpected default value type: %T", x))
	}
}

// ModeInspectSchema returns the InspectMode or its default.
func ModeInspectSchema(o *InspectOptions) InspectMode {
	if o == nil || o.Mode == 0 {
		return InspectSchemas | InspectTables
	}
	return o.Mode
}

// ModeInspectRealm returns the InspectMode or its default.
func ModeInspectRealm(o *InspectRealmOption) InspectMode {
	if o == nil || o.Mode == 0 {
		return InspectSchemas | InspectTables
	}
	return o.Mode
}

// ExcludeRealm filters resources in the realm based on the given patterns.
func ExcludeRealm(r *Realm, patterns []string) (*Realm, error) {
	if len(patterns) == 0 {
		return r, nil
	}
	var schemas []*Schema
	globs, err := split(patterns)
	if err != nil {
		return nil, err
	}
Filter:
	for _, s := range r.Schemas {
		for i, g := range globs {
			if len(g) > 3 {
				return nil, fmt.Errorf("too many parts in pattern: %q", patterns[i])
			}
			match, err := filepath.Match(g[0], s.Name)
			if err != nil {
				return nil, err
			}
			if match {
				// In case there is a match, and it is
				// a single glob we exclude this
				if len(g) == 1 {
					continue Filter
				}
				if err := excludeS(s, g[1:]); err != nil {
					return nil, err
				}
			}
		}
		schemas = append(schemas, s)
	}
	r.Schemas = schemas
	return r, nil
}

// ExcludeSchema filters resources in the schema based on the given patterns.
func ExcludeSchema(s *Schema, patterns []string) (*Schema, error) {
	if len(patterns) == 0 {
		return s, nil
	}
	if s.Realm == nil {
		return nil, fmt.Errorf("missing realm for schema %q", s.Name)
	}
	for i, p := range patterns {
		patterns[i] = fmt.Sprintf("%s.%s", s.Name, p)
	}
	if _, err := ExcludeRealm(s.Realm, patterns); err != nil {
		return nil, err
	}
	return s, nil
}

// split parses the list of patterns into chain of resource-globs.
// For example, 's*.t.*' is split to ['s*', 't', *].
func split(patterns []string) ([][]string, error) {
	globs := make([][]string, len(patterns))
	for i, p := range patterns {
		r := csv.NewReader(strings.NewReader(p))
		r.Comma = '.'
		switch parts, err := r.ReadAll(); {
		case err != nil:
			return nil, err
		case len(parts) != 1:
			return nil, fmt.Errorf("unexpected pattern: %q", p)
		case len(parts[0]) == 0:
			return nil, fmt.Errorf("empty pattern: %q", p)
		default:
			globs[i] = parts[0]
		}
	}
	return globs, nil
}

func excludeS(s *Schema, glob []string) error {
	var tables []*Table
	for _, t := range s.Tables {
		match, err := filepath.Match(glob[0], t.Name)
		if err != nil {
			return err
		}
		if match {
			// In case there is a match, and it is
			// a single glob we exclude this table.
			if len(glob) == 1 {
				continue
			}
			if err := excludeT(t, glob[1]); err != nil {
				return err
			}
		}
		// No match or glob has more than one pattern.
		tables = append(tables, t)
	}
	s.Tables = tables
	return nil
}

func excludeT(t *Table, pattern string) (err error) {
	ex := make(map[*Index]struct{})
	ef := make(map[*ForeignKey]struct{})

	t.Columns, err = filter(t.Columns, func(c *Column) (bool, error) {
		match, err := filepath.Match(pattern, c.Name)
		if !match || err != nil {
			return false, err
		}
		for _, idx := range c.Indexes {
			ex[idx] = struct{}{}
		}
		for _, fk := range c.ForeignKeys {
			ef[fk] = struct{}{}
		}
		return true, nil
	})

	if err != nil {
		return
	}

	t.Indexes, err = filter(t.Indexes, func(idx *Index) (bool, error) {
		if _, ok := ex[idx]; ok {
			return true, nil
		}
		return filepath.Match(pattern, idx.Name)
	})

	if err != nil {
		return
	}

	t.ForeignKeys, err = filter(t.ForeignKeys, func(fk *ForeignKey) (bool, error) {
		if _, ok := ef[fk]; ok {
			return true, nil
		}
		return filepath.Match(pattern, fk.Name)
	})

	return
}

func filter[T any](s []T, f func(T) (bool, error)) ([]T, error) {
	r := make([]T, 0, len(s))
	for i := range s {
		match, err := f(s[i])
		if err != nil {
			return nil, err
		}
		if !match {
			r = append(r, s[i])
		}
	}
	return r, nil
}