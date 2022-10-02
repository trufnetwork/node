package schema_test

import (
	"testing"

	"kwil/x/schemadef/schema"

	"github.com/stretchr/testify/require"
)

func TestTable_AddColumns(t *testing.T) {
	users := schema.NewTable("users").
		SetComment("users table").
		AddColumns(
			schema.NewBoolColumn("active", "bool"),
			schema.NewNullStringColumn("name", "varchar", schema.StringSize(255)),
		)
	require.Equal(
		t,
		&schema.Table{
			Name: "users",
			Attrs: []schema.Attr{
				&schema.Comment{Text: "users table"},
			},
			Columns: []*schema.Column{
				{Name: "active", Type: &schema.ColumnType{Type: &schema.BoolType{T: "bool"}}},
				{Name: "name", Type: &schema.ColumnType{Nullable: true, Type: &schema.StringType{T: "varchar", Size: 255}}},
			},
		},
		users,
	)
}

func TestSchema_AddTables(t *testing.T) {
	userColumns := []*schema.Column{
		schema.NewIntColumn("id", "int"),
		schema.NewBoolColumn("active", "boolean"),
		schema.NewNullStringColumn("name", "varchar", schema.StringSize(255)),
	}
	users := schema.NewTable("users").
		AddColumns(userColumns...).
		SetPrimaryKey(schema.NewPrimaryKey(userColumns[0])).
		SetComment("users table").
		AddIndexes(
			schema.NewUniqueIndex("unique_name").
				AddColumns(userColumns[2]).
				SetComment("index comment"),
		)
	postColumns := []*schema.Column{
		schema.NewIntColumn("id", "int"),
		schema.NewStringColumn("text", "longtext"),
		schema.NewNullIntColumn("author_id", "int"),
	}
	posts := schema.NewTable("posts").
		AddColumns(postColumns...).
		SetPrimaryKey(schema.NewPrimaryKey(postColumns[0])).
		SetComment("posts table").
		AddForeignKeys(
			schema.NewForeignKey("author_id").
				AddColumns(postColumns[2]).
				SetRefTable(users).
				AddRefColumns(userColumns[0]).
				SetOnDelete(schema.Cascade).
				SetOnUpdate(schema.SetNull),
		)
	require.Equal(
		t,
		func() *schema.Schema {
			s := &schema.Schema{Name: "public"}
			users := &schema.Table{
				Name:   "users",
				Schema: s,
				Attrs: []schema.Attr{
					&schema.Comment{Text: "users table"},
				},
				Columns: []*schema.Column{
					{Name: "id", Type: &schema.ColumnType{Type: &schema.IntegerType{T: "int"}}},
					{Name: "active", Type: &schema.ColumnType{Type: &schema.BoolType{T: "boolean"}}},
					{Name: "name", Type: &schema.ColumnType{Nullable: true, Type: &schema.StringType{T: "varchar", Size: 255}}},
				},
			}
			s.Tables = append(s.Tables, users)
			users.PrimaryKey = &schema.Index{Unique: true, Parts: []*schema.IndexPart{{Column: users.Columns[0]}}}
			users.PrimaryKey.Table = users
			users.Columns[0].Indexes = append(users.Columns[0].Indexes, users.PrimaryKey)
			users.Indexes = append(users.Indexes, &schema.Index{
				Name:   "unique_name",
				Unique: true,
				Parts:  []*schema.IndexPart{{Column: users.Columns[2]}},
				Attrs:  []schema.Attr{&schema.Comment{Text: "index comment"}},
			})
			users.Indexes[0].Table = users
			users.Columns[2].Indexes = users.Indexes

			posts := &schema.Table{
				Name:   "posts",
				Schema: s,
				Attrs: []schema.Attr{
					&schema.Comment{Text: "posts table"},
				},
				Columns: []*schema.Column{
					{Name: "id", Type: &schema.ColumnType{Type: &schema.IntegerType{T: "int"}}},
					{Name: "text", Type: &schema.ColumnType{Type: &schema.StringType{T: "longtext"}}},
					{Name: "author_id", Type: &schema.ColumnType{Nullable: true, Type: &schema.IntegerType{T: "int"}}},
				},
			}
			s.Tables = append(s.Tables, posts)
			posts.PrimaryKey = &schema.Index{Unique: true, Parts: []*schema.IndexPart{{Column: posts.Columns[0]}}}
			posts.PrimaryKey.Table = posts
			posts.Columns[0].Indexes = append(posts.Columns[0].Indexes, posts.PrimaryKey)
			posts.ForeignKeys = append(posts.ForeignKeys, &schema.ForeignKey{
				Symbol:     "author_id",
				Table:      posts,
				Columns:    posts.Columns[2:],
				RefTable:   users,
				RefColumns: users.Columns[0:1],
				OnDelete:   schema.Cascade,
				OnUpdate:   schema.SetNull,
			})
			posts.Columns[2].ForeignKeys = posts.ForeignKeys
			return s
		}(),
		schema.New("public").AddTables(users, posts),
	)
}

func TestSchema_SetCharset(t *testing.T) {
	s := schema.New("public")
	require.Empty(t, s.Attrs)
	s.SetCharset("utf8mb4")
	require.Len(t, s.Attrs, 1)
	require.Equal(t, &schema.Charset{V: "utf8mb4"}, s.Attrs[0])
	s.SetCharset("latin1")
	require.Len(t, s.Attrs, 1)
	require.Equal(t, &schema.Charset{V: "latin1"}, s.Attrs[0])
	s.UnsetCharset()
	require.Empty(t, s.Attrs)
}

func TestSchema_SetCollation(t *testing.T) {
	s := schema.New("public")
	require.Empty(t, s.Attrs)
	s.SetCollation("utf8mb4_general_ci")
	require.Len(t, s.Attrs, 1)
	require.Equal(t, &schema.Collation{V: "utf8mb4_general_ci"}, s.Attrs[0])
	s.SetCollation("latin1_swedish_ci")
	require.Len(t, s.Attrs, 1)
	require.Equal(t, &schema.Collation{V: "latin1_swedish_ci"}, s.Attrs[0])
	s.UnsetCollation()
	require.Empty(t, s.Attrs)
}

func TestSchema_SetComment(t *testing.T) {
	s := schema.New("public")
	require.Empty(t, s.Attrs)
	s.SetComment("1")
	require.Len(t, s.Attrs, 1)
	require.Equal(t, &schema.Comment{Text: "1"}, s.Attrs[0])
	s.SetComment("2")
	require.Len(t, s.Attrs, 1)
	require.Equal(t, &schema.Comment{Text: "2"}, s.Attrs[0])
}
