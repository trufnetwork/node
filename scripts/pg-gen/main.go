package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/format"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	pgx "github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
)

// https://dba.stackexchange.com/questions/255412/how-to-select-functions-that-belong-in-a-given-extension-in-postgresql
//
// Extension functions are added to the public schema
const extensionFuncs = `
WITH extension_funcs AS (
  SELECT p.oid
  FROM pg_catalog.pg_extension AS e
      INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
      INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
      INNER JOIN pg_catalog.pg_namespace AS ne ON (ne.oid = e.extnamespace)
      INNER JOIN pg_catalog.pg_namespace AS np ON (np.oid = p.pronamespace)
  WHERE d.deptype = 'e' AND e.extname = $1
)
SELECT p.proname as name,
  format_type(p.prorettype, NULL),
  array(select format_type(unnest(p.proargtypes), NULL)),
  p.proargnames,
  p.proargnames[p.pronargs-p.pronargdefaults+1:p.pronargs],
  p.proargmodes::text[]
FROM pg_catalog.pg_proc p
JOIN extension_funcs ef ON ef.oid = p.oid
WHERE pg_function_is_visible(p.oid)
-- simply order all columns to keep subsequent runs stable
ORDER BY 1, 2, 3, 4, 5;
`

const catalogTmpl = `
// Code generated by sqlc-pg-gen. DO NOT EDIT.

package {{.Pkg}}

import (
	"github.com/kwilteam/kwil-db/internal/sqlparse/ast"
	"github.com/kwilteam/kwil-db/internal/catalog"
)

{{- $funcName := .GenFnName }}
{{- range $i, $procs := .Procs }}
func {{ $funcName }}Funcs{{ $i }}() []*catalog.Function {
	return []*catalog.Function{
	    {{- range $procs }}
		{
			Name: "{{.Name}}",
			Args: []*catalog.Argument{
				{{range .Args}}{
				{{- if .Name}}
				Name: "{{.Name}}",
				{{- end}}
				{{- if .HasDefault}}
				HasDefault: true,
				{{- end}}
				Type: &ast.TypeName{Name: "{{.TypeName}}"},
				{{- if ne .Mode "i" }}
				Mode: {{ .GoMode }},
				{{- end}}
				},
				{{end}}
			},
			ReturnType: &ast.TypeName{Name: "{{.ReturnTypeName}}"},
		},
		{{- end}}
	}
}
{{ end }}

func {{.GenFnName}}Funcs() []*catalog.Function {
	funcs := []*catalog.Function{}
	{{- range $i, $procs := .Procs }}
	funcs = append(funcs, {{$funcName}}Funcs{{$i}}()...)
	{{- end }}
	return funcs
}

func {{.GenFnName}}() *catalog.Schema {
	s := &catalog.Schema{Name: "{{ .SchemaName }}"}
	s.Funcs = {{.GenFnName}}Funcs()
	{{- if .Relations }}
	s.Tables = []*catalog.Table {
	    {{- range .Relations }}
		{
			Rel: &ast.TableName{
				Catalog: "{{.Catalog}}",
				Schema: "{{.SchemaName}}",
				Name: "{{.Name}}",
			},
			Columns: []*catalog.Column{
				{{- range .Columns}}
				{
					Name: "{{.Name}}",
					Type: ast.TypeName{Name: "{{.Type}}"},
					{{- if .IsNotNull}}
					IsNotNull: true,
					{{- end}}
					{{- if .IsArray}}
					IsArray: true,
					{{- end}}
					{{- if .Length }}
					Length: toPointer({{ .Length }}),
					{{- end}}
				},
				{{- end}}
			},
		},
		{{- end}}
	}
	{{- end }}
	return s
}
`

const loaderFuncTmpl = `
// Code generated by sqlc-pg-gen. DO NOT EDIT.

package postgres

import (
	"github.com/kwilteam/kwil-db/internal/sqlparse/postgres/contrib"
	"github.com/kwilteam/kwil-db/internal/catalog"
)

func loadExtension(name string) *catalog.Schema {
	switch name {
	{{- range .}}
	case "{{.Name}}":
		return contrib.{{.Func}}()
	{{- end}}
	}
	return nil
}
`

type tmplCtx struct {
	Pkg        string
	GenFnName  string
	SchemaName string
	Procs      [][]Proc
	Relations  []Relation
}

func main() {
	var opts struct {
		OutputDir   string
		DatabaseUrl string
	}

	cmd := &cobra.Command{
		Use:           "pg-gen",
		Short:         "pg-gen is a an internal tool to generate postgres catalog",
		Long:          "",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.DatabaseUrl == "" {
				return errors.New("missing required flag: -u,--url")
			}

			return run(cmd.Context(), opts.OutputDir, opts.DatabaseUrl)
		},
	}

	// defaultOutput := filepath.Join("internal", "sqlparse", "engine", "postgres")

	cmd.Flags().StringVarP(&opts.OutputDir, "output", "o", "", "output directory")
	cmd.Flags().StringVarP(&opts.DatabaseUrl, "url", "u", os.Getenv("DATABASE_URL"), "database url")
	cmd.MarkFlagRequired("output")

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func clean(arg string) string {
	arg = strings.TrimSpace(arg)
	arg = strings.ReplaceAll(arg, "\"any\"", "any")
	arg = strings.ReplaceAll(arg, "\"char\"", "char")
	arg = strings.ReplaceAll(arg, "\"timestamp\"", "char")
	return arg
}

// writeFormattedGo executes `tmpl` with `data` as its context to the the file `destPath`
func writeFormattedGo(tmpl *template.Template, data any, destPath string) error {
	out := bytes.NewBuffer([]byte{})
	err := tmpl.Execute(out, data)
	if err != nil {
		return err
	}
	code, err := format.Source(out.Bytes())
	if err != nil {
		return err
	}

	err = os.WriteFile(destPath, code, 0644)
	if err != nil {
		return err
	}

	return nil
}

// preserveLegacyCatalogBehavior maintain previous ordering and filtering
// that was manually done to the generated file pg_catalog.go.
// Some of the test depend on this ordering - in particular, function lookups
// where there might be multiple matching functions (due to type overloads)
// Until sqlc supports "smarter" looking up of these functions,
// preserveLegacyCatalogBehavior ensures there are no accidental test breakages
func preserveLegacyCatalogBehavior(allProcs []Proc) []Proc {
	// Preserve the legacy sort order of the end-to-end tests
	sort.SliceStable(allProcs, func(i, j int) bool {
		fnA := allProcs[i]
		fnB := allProcs[j]

		if fnA.Name == "lower" && fnB.Name == "lower" && len(fnA.ArgTypes) == 1 && fnA.ArgTypes[0] == "text" {
			return true
		}

		if fnA.Name == "generate_series" && fnB.Name == "generate_series" && len(fnA.ArgTypes) == 2 && fnA.ArgTypes[0] == "numeric" {
			return true
		}

		return false
	})

	procs := make([]Proc, 0, len(allProcs))
	for _, p := range allProcs {
		// Skip generating pg_catalog.concat to preserve legacy behavior
		if p.Name == "concat" {
			continue
		}

		procs = append(procs, p)
	}

	return procs
}

func chunkSlice[T any](slice []T, chunkSize int) [][]T {
	var chunks [][]T
	for {
		if len(slice) == 0 {
			break
		}

		// necessary check to avoid slicing beyond
		// slice capacity
		if len(slice) < chunkSize {
			chunkSize = len(slice)
		}

		chunks = append(chunks, slice[0:chunkSize])
		slice = slice[chunkSize:]
	}

	return chunks
}

func run(ctx context.Context, output, url string) error {
	tmpl, err := template.New("").Parse(catalogTmpl)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	schemas := []schemaToLoad{
		{
			Name:      "pg_catalog",
			GenFnName: "genPGCatalog",
			DestPath:  filepath.Join(output, "pg_catalog.go"),
		},
		{
			Name:      "information_schema",
			GenFnName: "genInformationSchema",
			DestPath:  filepath.Join(output, "information_schema.go"),
		},
	}

	for _, schema := range schemas {
		procs, err := readProcs(ctx, conn, schema.Name)
		if err != nil {
			return err
		}

		if schema.Name == "pg_catalog" {
			procs = preserveLegacyCatalogBehavior(procs)
		}

		relations, err := readRelations(ctx, conn, schema.Name)
		if err != nil {
			return err
		}

		err = writeFormattedGo(tmpl, tmplCtx{
			Pkg:        "postgres",
			SchemaName: schema.Name,
			GenFnName:  schema.GenFnName,
			Procs:      chunkSlice(procs, 500),
			Relations:  relations,
		}, schema.DestPath)

		if err != nil {
			return err
		}
	}

	loaded := []extensionPair{}
	for _, extension := range extensions {
		name := strings.Replace(extension, "-", "_", -1)

		var funcName string
		for _, part := range strings.Split(name, "_") {
			funcName += strings.Title(part)
		}

		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS \"%s\"", extension))
		if err != nil {
			log.Printf("error creating %s: %s", extension, err)
			continue
		}

		rows, err := conn.Query(ctx, extensionFuncs, extension)
		if err != nil {
			return err
		}
		procs, err := scanProcs(rows)
		if err != nil {
			return err
		}
		if len(procs) == 0 {
			log.Printf("no functions in %s, skipping", extension)
			continue
		}

		// Preserve the legacy sort order of the end-to-end tests
		sort.SliceStable(procs, func(i, j int) bool {
			fnA := procs[i]
			fnB := procs[j]

			if extension == "pgcrypto" {
				if fnA.Name == "digest" && fnB.Name == "digest" && len(fnA.ArgTypes) == 2 && fnA.ArgTypes[0] == "text" {
					return true
				}
			}

			return false
		})

		extensionPath := filepath.Join(output, "contrib", name+".go")
		err = writeFormattedGo(tmpl, tmplCtx{
			Pkg:        "contrib",
			SchemaName: "pg_catalog",
			GenFnName:  funcName,
			Procs:      chunkSlice(procs, 500),
		}, extensionPath)
		if err != nil {
			return fmt.Errorf("error generating extension %s: %w", extension, err)
		}

		loaded = append(loaded, extensionPair{Name: extension, Func: funcName})
	}

	extensionTmpl, err := template.New("").Parse(loaderFuncTmpl)
	if err != nil {
		return err
	}

	extensionLoaderPath := filepath.Join(output, "extension.go")
	err = writeFormattedGo(extensionTmpl, loaded, extensionLoaderPath)
	if err != nil {
		return err
	}

	return nil
}

type schemaToLoad struct {
	// name is the name of a schema to load
	Name string
	// DestPath is the desination for the generate file
	DestPath string
	// The name of the function to generate for loading this schema
	GenFnName string
}

type extensionPair struct {
	Name string
	Func string
}

// https://www.postgresql.org/docs/current/contrib.html
var extensions = []string{
	"adminpack",
	"amcheck",
	"auth_delay",
	"auto_explain",
	"bloom",
	"btree_gin",
	"btree_gist",
	"citext",
	"cube",
	"dblink",
	"dict_int",
	"dict_xsyn",
	"earthdistance",
	"file_fdw",
	"fuzzystrmatch",
	"hstore",
	"intagg",
	"intarray",
	"isn",
	"lo",
	"ltree",
	"pageinspect",
	"passwordcheck",
	"pg_buffercache",
	"pgcrypto",
	"pg_freespacemap",
	"pg_prewarm",
	"pgrowlocks",
	"pg_stat_statements",
	"pgstattuple",
	"pg_trgm",
	"pg_visibility",
	"postgres_fdw",
	"seg",
	"sepgsql",
	"spi",
	"sslinfo",
	"tablefunc",
	"tcn",
	"test_decoding",
	"tsm_system_rows",
	"tsm_system_time",
	"unaccent",
	"uuid-ossp",
	"xml2",
}
