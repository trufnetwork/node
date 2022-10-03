package hclspec

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"kwil/x/schemadef/hcl"
	"kwil/x/schemadef/schema"
)

// List of convert function types.
type (
	ColumnSpecFunc     func(*schema.Column, *schema.Table) (*Column, error)
	ColumnTypeSpecFunc func(schema.Type) (*Column, error)
	TypeSpecFunc       func(schema.Type) (*hcl.Type, error)
	TableSpecFunc      func(*schema.Table) (*Table, error)
	PrimaryKeySpecFunc func(*schema.Index) (*PrimaryKey, error)
	IndexSpecFunc      func(*schema.Index) (*Index, error)
	ForeignKeySpecFunc func(*schema.ForeignKey) (*ForeignKey, error)
	CheckSpecFunc      func(*schema.Check) *Check

	SchemaTableConverter interface {
		ConvertTable(*Table, *schema.Schema) (*schema.Table, error)
	}

	SchemaQueryConverter interface {
		ConvertQuery(*Query, *schema.Realm) (*schema.Query, error)
	}

	SchemaTypeConverter interface {
		ConvertType(*hcl.Type, ...*hcl.Attr) (schema.Type, error)
	}

	SchemaRoleConverter interface {
		ConvertRole(*Role, *schema.Realm) (*schema.Role, error)
	}

	SchemaEnumConverter interface {
		ConvertEnums([]*Table, []*Enum, *schema.Realm) error
	}

	SchemaConverter interface {
		SchemaTableConverter
		SchemaQueryConverter
		SchemaTypeConverter
		SchemaRoleConverter
		SchemaEnumConverter
	}
)

// Scan populates the Realm from the schemas and table specs.
func Scan(db *schema.Realm, doc *Realm, conv SchemaConverter) error {
	// Build the schemas.
	for _, schemaSpec := range doc.Schemas {
		sch := &schema.Schema{Name: schemaSpec.Name, Realm: db}
		for _, tableSpec := range doc.Tables {
			name, err := SchemaName(tableSpec.Schema)
			if err != nil {
				return fmt.Errorf("spec: cannot extract schema name for table %q: %w", tableSpec.Name, err)
			}
			if name == schemaSpec.Name {
				tbl, err := conv.ConvertTable(tableSpec, sch)
				if err != nil {
					return err
				}
				sch.Tables = append(sch.Tables, tbl)
			}
		}
		db.Schemas = append(db.Schemas, sch)
	}
	// Link the foreign keys.
	for _, sch := range db.Schemas {
		for _, tbl := range sch.Tables {
			tableSpec, err := findTableSpec(doc.Tables, sch.Name, tbl.Name)
			if err != nil {
				return err
			}
			if err := linkForeignKeys(tbl, sch, tableSpec); err != nil {
				return err
			}
		}
	}

	// Build the queries.
	for _, querySpec := range doc.Queries {
		q, err := conv.ConvertQuery(querySpec, db)
		if err != nil {
			return err
		}
		db.Queries = append(db.Queries, q)
	}

	// Build the roles.
	for _, roleSpec := range doc.Roles {
		r, err := conv.ConvertRole(roleSpec, db)
		if err != nil {
			return err
		}
		db.Roles = append(db.Roles, r)
	}

	if len(doc.Enums) > 0 {
		if err := conv.ConvertEnums(doc.Tables, doc.Enums, db); err != nil {
			return err
		}
		for _, e := range doc.Enums {
			if err := loadEnum(db, e); err != nil {
				return err
			}
		}
	}

	return nil
}

func loadEnum(r *schema.Realm, e *Enum) error {
	schemaName, err := SchemaName(e.Schema)
	if err != nil {
		return err
	}
	s, ok := r.Schema(schemaName)
	if !ok {
		return fmt.Errorf("spec: schema %q not found", schemaName)
	}

	out := &schema.Enum{Name: e.Name}
	out.Values = append(out.Values, e.Values...)
	s.AddEnums(out)
	return nil
}

func ToRole(r *Role, db *schema.Realm) (*schema.Role, error) {
	queries := make([]*schema.Query, len(r.Queries))
	for i := range queries {
		q, err := QueryByRef(db, r.Queries[i])
		if err != nil {
			return nil, err
		}
		queries[i] = q
	}
	return &schema.Role{
		Name:    r.Name,
		Realm:   db,
		Queries: queries,
		Default: r.Default,
	}, nil
}

func ToQuery(q *Query, db *schema.Realm) (*schema.Query, error) {
	return &schema.Query{
		Name:      q.Name,
		Realm:     db,
		Statement: q.Statement,
	}, nil
}

// ToColumn converts a Column into a schema.Column.
func ToColumn(s *Column, conv SchemaTypeConverter) (*schema.Column, error) {
	out := &schema.Column{
		Name: s.Name,
		Type: &schema.ColumnType{
			Nullable: s.Nullable,
		},
	}
	if s.Default != nil {
		switch d := s.Default.(type) {
		case *hcl.LiteralValue:
			out.Default = &schema.Literal{V: d.V}
		case *hcl.RawExpr:
			out.Default = &schema.RawExpr{X: d.X}
		default:
			return nil, fmt.Errorf("unsupported value type for default: %T", d)
		}
	}
	ct, err := conv.ConvertType(s.Type, s.Extra.Attrs...)
	if err != nil {
		return nil, err
	}

	out.Type.Type = ct
	if err := ConvertCommentFromSpec(s, &out.Attrs); err != nil {
		return nil, err
	}
	return out, err
}

// ToIndex converts a Index to a schema.Index. The optional arguments allow
// passing functions for mutating the created index-part (e.g. add attributes).
func ToIndex(s *Index, parent *schema.Table, partFns ...func(*IndexPart, *schema.IndexPart) error) (*schema.Index, error) {
	parts := make([]*schema.IndexPart, 0, len(s.Columns)+len(s.Parts))
	switch n, m := len(s.Columns), len(s.Parts); {
	case n == 0 && m == 0:
		return nil, fmt.Errorf("missing definition for index %q", s.Name)
	case n > 0 && m > 0:
		return nil, fmt.Errorf(`multiple definitions for index %q, use "columns" or "on"`, s.Name)
	case n > 0:
		for i, c := range s.Columns {
			c, err := ColumnByRef(parent, c)
			if err != nil {
				return nil, err
			}
			parts = append(parts, &schema.IndexPart{
				Seq:    i,
				Column: c,
			})
		}
	case m > 0:
		for i, p := range s.Parts {
			part := &schema.IndexPart{Seq: i, Descending: p.Desc}
			switch {
			case p.Column == nil && p.Expr == "":
				return nil, fmt.Errorf(`"column" or "expr" are required for index %q at position %d`, s.Name, i)
			case p.Column != nil && p.Expr != "":
				return nil, fmt.Errorf(`cannot use both "column" and "expr" in index %q at position %d`, s.Name, i)
			case p.Expr != "":
				part.Expr = &schema.RawExpr{X: p.Expr}
			case p.Column != nil:
				c, err := ColumnByRef(parent, p.Column)
				if err != nil {
					return nil, err
				}
				part.Column = c
			}
			for _, f := range partFns {
				if err := f(p, part); err != nil {
					return nil, err
				}
			}
			parts = append(parts, part)
		}
	}
	i := &schema.Index{
		Name:   s.Name,
		Unique: s.Unique,
		Table:  parent,
		Parts:  parts,
	}
	if err := ConvertCommentFromSpec(s, &i.Attrs); err != nil {
		return nil, err
	}
	return i, nil
}

func FromQuery(q *schema.Query) (*Query, error) {
	return &Query{
		Name:      q.Name,
		Statement: q.Statement,
	}, nil
}

// FromColumn converts a *schema.Column into a *Column using the ColumnTypeSpecFunc.
func FromColumn(col *schema.Column, columnTypeSpec ColumnTypeSpecFunc) (*Column, error) {
	ct, err := columnTypeSpec(col.Type.Type)
	if err != nil {
		return nil, err
	}
	s := &Column{
		Name:     col.Name,
		Type:     ct.Type,
		Nullable: col.Type.Nullable,
		DefaultExtension: hcl.DefaultExtension{
			Extra: hcl.Resource{Attrs: ct.DefaultExtension.Extra.Attrs},
		},
	}
	if col.Default != nil {
		lv, err := toValue(col.Default)
		if err != nil {
			return nil, err
		}
		s.Default = lv
	}
	convertCommentFromSchema(col.Attrs, &s.Extra.Attrs)
	return s, nil
}

func FromRole(r *schema.Role) (*Role, error) {
	queries := make([]*hcl.Ref, len(r.Queries))
	for i := range queries {
		queries[i] = QueryRef(r.Queries[i].Name)
	}
	return &Role{
		Name:    r.Name,
		Queries: queries,
		Default: r.Default,
	}, nil
}

// findTableSpec searches tableSpecs for a spec of a table named tableName in a schema named schemaName.
func findTableSpec(tableSpecs []*Table, schemaName, tableName string) (*Table, error) {
	for _, tbl := range tableSpecs {
		n, err := SchemaName(tbl.Schema)
		if err != nil {
			return nil, err
		}
		if n == schemaName && tbl.Name == tableName {
			return tbl, nil
		}
	}
	return nil, fmt.Errorf("table %s.%s not found", schemaName, tableName)
}

// linkForeignKeys creates the foreign keys defined in the Table's spec by creating references
// to column in the provided Schema. It is assumed that all tables referenced FK definitions in the spec
// are reachable from the provided schema or its connected realm.
func linkForeignKeys(tbl *schema.Table, sch *schema.Schema, table *Table) error {
	for _, s := range table.ForeignKeys {
		fk := &schema.ForeignKey{Name: s.Symbol, Table: tbl}
		if s.OnUpdate != nil {
			fk.OnUpdate = schema.ReferenceOption(FromVar(s.OnUpdate.V))
		}
		if s.OnDelete != nil {
			fk.OnDelete = schema.ReferenceOption(FromVar(s.OnDelete.V))
		}
		if n, m := len(s.Columns), len(s.RefColumns); n != m {
			return fmt.Errorf("sqlspec: number of referencing and referenced columns do not match for foreign-key %q", fk.Name)
		}
		for _, ref := range s.Columns {
			c, err := ColumnByRef(tbl, ref)
			if err != nil {
				return err
			}
			fk.Columns = append(fk.Columns, c)
		}
		for i, ref := range s.RefColumns {
			t, c, err := externalRef(ref, sch)
			if isLocalRef(ref) {
				t = fk.Table
				c, err = ColumnByRef(fk.Table, ref)
			}
			if err != nil {
				return err
			}
			if i > 0 && fk.RefTable != t {
				return fmt.Errorf("sqlspec: more than 1 table was referenced for foreign-key %q", fk.Name)
			}
			fk.RefTable = t
			fk.RefColumns = append(fk.RefColumns, c)
		}
		tbl.ForeignKeys = append(tbl.ForeignKeys, fk)
	}
	return nil
}

// FromSchema converts a schema.Schema into Schema and []Table.
func FromSchema(s *schema.Schema, fn TableSpecFunc) (*Schema, []*Table, error) {
	ss := &Schema{
		Name: s.Name,
	}
	tables := make([]*Table, 0, len(s.Tables))
	for _, t := range s.Tables {
		table, err := fn(t)
		if err != nil {
			return nil, nil, err
		}
		if s.Name != "" {
			table.Schema = SchemaRef(s.Name)
		}
		tables = append(tables, table)
	}
	return ss, tables, nil
}

// FromTable converts a schema.Table to a Table.
func FromTable(t *schema.Table, colFn ColumnSpecFunc, pkFn PrimaryKeySpecFunc, idxFn IndexSpecFunc, fkFn ForeignKeySpecFunc, ckFn CheckSpecFunc) (*Table, error) {
	s := &Table{
		Name: t.Name,
	}
	for _, c := range t.Columns {
		col, err := colFn(c, t)
		if err != nil {
			return nil, err
		}
		s.Columns = append(s.Columns, col)
	}
	if t.PrimaryKey != nil {
		pk, err := pkFn(t.PrimaryKey)
		if err != nil {
			return nil, err
		}
		s.PrimaryKey = pk
	}
	for _, idx := range t.Indexes {
		i, err := idxFn(idx)
		if err != nil {
			return nil, err
		}
		s.Indexes = append(s.Indexes, i)
	}
	for _, fk := range t.ForeignKeys {
		f, err := fkFn(fk)
		if err != nil {
			return nil, err
		}
		s.ForeignKeys = append(s.ForeignKeys, f)
	}
	for _, attr := range t.Attrs {
		if c, ok := attr.(*schema.Check); ok {
			s.Checks = append(s.Checks, ckFn(c))
		}
	}
	convertCommentFromSchema(t.Attrs, &s.Extra.Attrs)
	return s, nil
}

// FromPrimaryKey converts schema.Index to a PrimaryKey.
func FromPrimaryKey(s *schema.Index) (*PrimaryKey, error) {
	c := make([]*hcl.Ref, 0, len(s.Parts))
	for _, v := range s.Parts {
		c = append(c, ColumnRef(v.Column.Name))
	}
	return &PrimaryKey{
		Columns: c,
	}, nil
}

// FromGenExpr returns the spec for a generated expression.
func FromGenExpr(x schema.GeneratedExpr, t func(string) string) *hcl.Resource {
	return &hcl.Resource{
		Type: "as",
		Attrs: []*hcl.Attr{
			StrAttr("expr", x.Expr),
			VarAttr("type", t(x.Type)),
		},
	}
}

// ConvertGenExpr converts the "as" attribute or the block under the given resource.
func ConvertGenExpr(r *hcl.Resource, c *schema.Column, t func(string) string) error {
	asA, okA := r.Attr("as")
	asR, okR := r.Resource("as")
	switch {
	case okA && okR:
		return fmt.Errorf("multiple as definitions for column %q", c.Name)
	case okA:
		expr, err := asA.String()
		if err != nil {
			return err
		}
		c.Attrs = append(c.Attrs, &schema.GeneratedExpr{
			Type: t(""), // default type.
			Expr: expr,
		})
	case okR:
		var spec struct {
			Expr string `spec:"expr"`
			Type string `spec:"type"`
		}
		if err := asR.As(&spec); err != nil {
			return err
		}
		c.Attrs = append(c.Attrs, &schema.GeneratedExpr{
			Expr: spec.Expr,
			Type: t(spec.Type),
		})
	}
	return nil
}

func toValue(expr schema.Expr) (hcl.Value, error) {
	var (
		v   string
		err error
	)
	switch expr := expr.(type) {
	case *schema.RawExpr:
		return &hcl.RawExpr{X: expr.X}, nil
	case *schema.Literal:
		v, err = normalizeQuotes(expr.V)
		if err != nil {
			return nil, err
		}
		return &hcl.LiteralValue{V: v}, nil
	default:
		return nil, fmt.Errorf("converting expr %T to literal value", expr)
	}
}

func normalizeQuotes(s string) (string, error) {
	if len(s) < 2 {
		return s, nil
	}
	// If string is quoted with single quotes:
	if strings.HasPrefix(s, `'`) && strings.HasSuffix(s, `'`) {
		uq := strings.ReplaceAll(s[1:len(s)-1], "''", "'")
		return strconv.Quote(uq), nil
	}
	return s, nil
}

// FromIndex converts schema.Index to Index.
func FromIndex(idx *schema.Index, partFns ...func(*schema.IndexPart, *IndexPart)) (*Index, error) {
	ss := &Index{Name: idx.Name, Unique: idx.Unique}
	convertCommentFromSchema(idx.Attrs, &ss.Extra.Attrs)
	if parts, ok := columnsOnly(idx); ok {
		ss.Columns = parts
		return ss, nil
	}
	ss.Parts = make([]*IndexPart, len(idx.Parts))
	for i, p := range idx.Parts {
		part := &IndexPart{Desc: p.Descending}
		switch {
		case p.Column == nil && p.Expr == nil:
			return nil, fmt.Errorf("missing column or expression for key part of index %q", idx.Name)
		case p.Column != nil && p.Expr != nil:
			return nil, fmt.Errorf("multiple key part definitions for index %q", idx.Name)
		case p.Column != nil:
			part.Column = ColumnRef(p.Column.Name)
		case p.Expr != nil:
			x, ok := p.Expr.(*schema.RawExpr)
			if !ok {
				return nil, fmt.Errorf("unexpected expression %T for index %q", p.Expr, idx.Name)
			}
			part.Expr = x.X
		}
		for _, f := range partFns {
			f(p, part)
		}
		ss.Parts[i] = part
	}
	return ss, nil
}

func columnsOnly(idx *schema.Index) ([]*hcl.Ref, bool) {
	parts := make([]*hcl.Ref, len(idx.Parts))
	for i, p := range idx.Parts {
		if p.Column == nil || p.Descending {
			return nil, false
		}
		parts[i] = ColumnRef(p.Column.Name)
	}
	return parts, true
}

// FromForeignKey converts schema.ForeignKey to ForeignKey.
func FromForeignKey(s *schema.ForeignKey) (*ForeignKey, error) {
	c := make([]*hcl.Ref, 0, len(s.Columns))
	for _, v := range s.Columns {
		c = append(c, ColumnRef(v.Name))
	}
	r := make([]*hcl.Ref, 0, len(s.RefColumns))
	for _, v := range s.RefColumns {
		ref := ColumnRef(v.Name)
		if s.Table != s.RefTable {
			ref = externalColRef(v.Name, s.RefTable.Name)
		}
		r = append(r, ref)
	}
	fk := &ForeignKey{
		Symbol:     s.Name,
		Columns:    c,
		RefColumns: r,
	}
	if s.OnUpdate != "" {
		fk.OnUpdate = &hcl.Ref{V: ToVar(string(s.OnUpdate))}
	}
	if s.OnDelete != "" {
		fk.OnDelete = &hcl.Ref{V: ToVar(string(s.OnDelete))}
	}
	return fk, nil
}

// SchemaName returns the name from a ref to a schema.
func SchemaName(ref *hcl.Ref) (string, error) {
	if ref == nil {
		return "", errors.New("missing 'schema' attribute")
	}
	parts := strings.Split(ref.V, ".")
	if len(parts) < 2 || parts[0] != "$schema" {
		return "", errors.New("expected ref format of $schema.name")
	}
	return parts[1], nil
}

// QueryByRef returns a query by its reference.
func QueryByRef(db *schema.Realm, ref *hcl.Ref) (*schema.Query, error) {
	s := strings.Split(ref.V, "$query.")
	if len(s) != 2 {
		return nil, fmt.Errorf("spec: failed to extract query name from %q", ref)
	}

	if c, ok := db.Query(s[1]); ok {
		return c, nil
	}
	return nil, fmt.Errorf("spec: unknown query %q", s[1])
}

// ColumnByRef returns a column from the table by its reference.
func ColumnByRef(t *schema.Table, ref *hcl.Ref) (*schema.Column, error) {
	s := strings.Split(ref.V, "$column.")
	if len(s) != 2 {
		return nil, fmt.Errorf("spec: failed to extract column name from %q", ref)
	}
	c, ok := t.Column(s[1])
	if !ok {
		return nil, fmt.Errorf("spec: unknown column %q in table %q", s[1], t.Name)
	}
	return c, nil
}

func externalRef(ref *hcl.Ref, sch *schema.Schema) (*schema.Table, *schema.Column, error) {
	tbl, err := findTable(ref, sch)
	if err != nil {
		return nil, nil, err
	}
	c, err := ColumnByRef(tbl, ref)
	if err != nil {
		return nil, nil, err
	}
	return tbl, c, nil
}

// findTable finds the table referenced by ref in the provided schema. If the table
// is not in the provided schema.Schema other schemas in the connected schema.Realm
// are searched as well.
func findTable(ref *hcl.Ref, sch *schema.Schema) (*schema.Table, error) {
	qualifier, tblName, err := tableName(ref)
	if err != nil {
		return nil, err
	}
	// Search the same schema.
	if qualifier == "" || qualifier == sch.Name {
		tbl, ok := sch.Table(tblName)
		if !ok {
			return tbl, fmt.Errorf("sqlspec: table %q not found", tblName)
		}
		return tbl, nil
	}
	if sch.Realm == nil {
		return nil, fmt.Errorf("sqlspec: table %s.%s not found", qualifier, tblName)
	}
	// Search for the table in another schemas in the realm.
	sch, ok := sch.Realm.Schema(qualifier)
	if !ok {
		return nil, fmt.Errorf("sqlspec: schema %q not found", qualifier)
	}
	tbl, ok := sch.Table(tblName)
	if !ok {
		return tbl, fmt.Errorf("sqlspec: table %q not found", tblName)
	}
	return tbl, nil
}

func tableName(ref *hcl.Ref) (qualifier, name string, err error) {
	s := strings.Split(ref.V, "$column.")
	if len(s) != 2 {
		return "", "", fmt.Errorf("sqlspec: failed to split by column name from %q", ref)
	}
	table := strings.TrimSuffix(s[0], ".")
	s = strings.Split(table, ".")
	switch len(s) {
	case 2:
		name = s[1]
	case 3:
		qualifier, name = s[1], s[2]
	default:
		return "", "", fmt.Errorf("sqlspec: failed to extract table name from %q", s)
	}
	return
}

func isLocalRef(r *hcl.Ref) bool {
	return strings.HasPrefix(r.V, "$column")
}

// ColumnRef returns the reference of a column by its name.
func ColumnRef(cName string) *hcl.Ref {
	return &hcl.Ref{V: "$column." + cName}
}

// QueryRef returns the reference of a query by its name.
func QueryRef(name string) *hcl.Ref {
	return &hcl.Ref{V: "$query." + name}
}

func externalColRef(cName string, tName string) *hcl.Ref {
	return &hcl.Ref{V: "$table." + tName + ".$column." + cName}
}

func qualifiedExternalColRef(cName, tName, sName string) *hcl.Ref {
	return &hcl.Ref{V: "$table." + sName + "." + tName + ".$column." + cName}
}

// SchemaRef returns the hcl.Ref to the schema with the given name.
func SchemaRef(n string) *hcl.Ref {
	return &hcl.Ref{V: "$schema." + n}
}

// Attrer is the interface that wraps the Attr method.
type Attrer interface {
	Attr(string) (*hcl.Attr, bool)
}

// ConvertCommentFromSpec converts a spec comment attribute to a schema element attribute.
func ConvertCommentFromSpec(s Attrer, attrs *[]schema.Attr) error {
	if c, ok := s.Attr("comment"); ok {
		s, err := c.String()
		if err != nil {
			return err
		}
		*attrs = append(*attrs, &schema.Comment{Text: s})
	}
	return nil
}

// convertCommentFromSchema converts a schema element comment attribute to a spec comment attribute.
func convertCommentFromSchema(src []schema.Attr, trgt *[]*hcl.Attr) {
	var c schema.Comment
	if schema.Has(src, &c) {
		*trgt = append(*trgt, StrAttr("comment", c.Text))
	}
}

// ReferenceVars holds the HCL variables
// for foreign keys' referential-actions.
var ReferenceVars = []string{
	ToVar(string(schema.NoAction)),
	ToVar(string(schema.Restrict)),
	ToVar(string(schema.Cascade)),
	ToVar(string(schema.SetNull)),
	ToVar(string(schema.SetDefault)),
}

// ToVar formats a string as variable to make it HCL compatible.
// The result is simple, replace each space with underscore.
func ToVar(s string) string { return strings.ReplaceAll(s, " ", "_") }

// FromVar is the inverse function of Var.
func FromVar(s string) string { return strings.ReplaceAll(s, "_", " ") }

// FromCheck converts schema.Check to Check.
func FromCheck(s *schema.Check) *Check {
	return &Check{
		Name: s.Name,
		Expr: s.Expr,
	}
}
