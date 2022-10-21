package engine

import (
	"fmt"
	"strconv"

	"kwil/x/sql/catalog"
	"kwil/x/sql/sqlerr"
	"kwil/x/sql/sqlparse/ast"
	"kwil/x/sql/sqlparse/astutils"
	"kwil/x/sql/sqlparse/named"
)

func (comp *Engine) resolveCatalogRefs(qc *QueryCatalog, rvs []*ast.RangeVar, args []paramRef, params *named.ParamSet) ([]Parameter, error) {
	c := comp.Catalog

	aliasMap := map[string]*catalog.QualName{}
	// TODO: Deprecate defaultTable
	var defaultTable *catalog.QualName
	var tables []*catalog.QualName

	typeMap := map[string]map[string]map[string]*catalog.Column{}
	indexTable := func(table *catalog.Table) error {
		tables = append(tables, table.QualName)
		if defaultTable == nil {
			defaultTable = table.QualName
		}
		schema := table.QualName.Schema
		if schema == "" {
			schema = c.DefaultSchema
		}
		if _, exists := typeMap[schema]; !exists {
			typeMap[schema] = map[string]map[string]*catalog.Column{}
		}
		typeMap[schema][table.QualName.Name] = map[string]*catalog.Column{}
		for _, c := range table.Columns {
			cc := c
			typeMap[schema][table.QualName.Name][c.Name] = cc
		}
		return nil
	}

	for _, rv := range rvs {
		if rv.Relname == nil {
			continue
		}
		fqn, err := ParseTableName(rv)
		if err != nil {
			return nil, err
		}
		if _, found := aliasMap[fqn.Name]; found {
			continue
		}
		table, ok := c.Table(fqn.Schema, fqn.Name)
		if !ok {
			// If the table name doesn't exist, fisrt check if it's a CTE
			if _, qcerr := qc.GetTable(fqn); qcerr != nil {
				return nil, err
			}
			continue
		}
		err = indexTable(table)
		if err != nil {
			return nil, err
		}
		if rv.Alias != nil {
			aliasMap[*rv.Alias.Aliasname] = &catalog.QualName{Catalog: fqn.Catalog, Schema: fqn.Schema, Name: fqn.Name}
		}
	}

	var a []Parameter
	for _, ref := range args {
		switch n := ref.parent.(type) {

		case *limitOffset:
			defaultP := named.NewInferredParam("offset", true)
			p, isNamed := params.FetchMerge(ref.ref.Number, defaultP)
			a = append(a, Parameter{
				Number: ref.ref.Number,
				Column: &Column{
					Name:         p.Name(),
					DataType:     "integer",
					NotNull:      p.NotNull(),
					IsNamedParam: isNamed,
				},
			})

		case *limitCount:
			defaultP := named.NewInferredParam("limit", true)
			p, isNamed := params.FetchMerge(ref.ref.Number, defaultP)
			a = append(a, Parameter{
				Number: ref.ref.Number,
				Column: &Column{
					Name:         p.Name(),
					DataType:     "integer",
					NotNull:      p.NotNull(),
					IsNamedParam: isNamed,
				},
			})

		case *ast.A_Expr:
			// TODO: While this works for a wide range of simple expressions,
			// more complicated expressions will cause this logic to fail.
			list := astutils.Search(n.Lexpr, func(node ast.Node) bool {
				_, ok := node.(*ast.ColumnRef)
				return ok
			})

			if len(list.Items) == 0 {
				// TODO: Move this to database-specific engine package
				dataType := "any"
				if astutils.Join(n.Name, ".") == "||" {
					dataType = "string"
				}

				defaultP := named.NewParam("")
				p, isNamed := params.FetchMerge(ref.ref.Number, defaultP)
				a = append(a, Parameter{
					Number: ref.ref.Number,
					Column: &Column{
						Name:         p.Name(),
						DataType:     dataType,
						IsNamedParam: isNamed,
						NotNull:      p.NotNull(),
					},
				})
				continue
			}

			switch left := list.Items[0].(type) {
			case *ast.ColumnRef:
				items := stringSlice(left.Fields)
				var key, alias string
				switch len(items) {
				case 1:
					key = items[0]
				case 2:
					alias = items[0]
					key = items[1]
				default:
					panic("too many field items: " + strconv.Itoa(len(items)))
				}

				search := tables
				if alias != "" {
					if original, ok := aliasMap[alias]; ok {
						search = []*catalog.QualName{original}
					} else {
						var located bool
						for _, fqn := range tables {
							if fqn.Name == alias {
								located = true
								search = []*catalog.QualName{fqn}
							}
						}
						if !located {
							return nil, &sqlerr.Error{
								Code:     "42703",
								Message:  fmt.Sprintf("table alias \"%s\" does not exist", alias),
								Location: left.Location,
							}
						}
					}
				}

				var found int
				for _, table := range search {
					schema := table.Schema
					if schema == "" {
						schema = c.DefaultSchema
					}
					if c, ok := typeMap[schema][table.Name][key]; ok {
						found += 1
						if ref.name != "" {
							key = ref.name
						}

						defaultP := named.NewInferredParam(key, c.IsNotNull)
						p, isNamed := params.FetchMerge(ref.ref.Number, defaultP)
						a = append(a, Parameter{
							Number: ref.ref.Number,
							Column: &Column{
								Name:         p.Name(),
								DataType:     dataType(c.Type),
								NotNull:      p.NotNull(),
								IsArray:      c.IsArray,
								Length:       c.Length,
								Table:        table,
								IsNamedParam: isNamed,
							},
						})
					}
				}

				if found == 0 {
					return nil, &sqlerr.Error{
						Code:     "42703",
						Message:  fmt.Sprintf("column \"%s\" does not exist", key),
						Location: left.Location,
					}
				}
				if found > 1 {
					return nil, &sqlerr.Error{
						Code:     "42703",
						Message:  fmt.Sprintf("column reference \"%s\" is ambiguous", key),
						Location: left.Location,
					}
				}
			}

		case *ast.BetweenExpr:
			if n == nil || n.Expr == nil || n.Left == nil || n.Right == nil {
				fmt.Println("ast.BetweenExpr is nil")
				continue
			}

			var key string
			if ref, ok := n.Expr.(*ast.ColumnRef); ok {
				itemsCount := len(ref.Fields.Items)
				if str, ok := ref.Fields.Items[itemsCount-1].(*ast.String); ok {
					key = str.Str
				}
			}

			number := 0
			if pr, ok := n.Left.(*ast.ParamRef); ok {
				number = pr.Number
			}

			for _, table := range tables {
				schema := table.Schema
				if schema == "" {
					schema = c.DefaultSchema
				}

				if c, ok := typeMap[schema][table.Name][key]; ok {
					defaultP := named.NewInferredParam(key, c.IsNotNull)
					p, isNamed := params.FetchMerge(ref.ref.Number, defaultP)
					a = append(a, Parameter{
						Number: number,
						Column: &Column{
							Name:         p.Name(),
							DataType:     dataType(c.Type),
							NotNull:      p.NotNull(),
							IsArray:      c.IsArray,
							Table:        table,
							IsNamedParam: isNamed,
						},
					})
				}
			}

		case *ast.FuncCall:
			fun, err := c.ResolveFuncCall(n)
			if err != nil {
				// Synthesize a function on the fly to avoid returning with an error
				// for an unknown Postgres function (e.g. defined in an extension)
				var args []*catalog.Argument
				for range n.Args.Items {
					args = append(args, &catalog.Argument{
						Type: &catalog.QualName{Name: "any"},
					})
				}
				fun = &catalog.Function{
					Name:       n.Func.Name,
					Args:       args,
					ReturnType: &catalog.QualName{Name: "any"},
				}
			}
			for i, item := range n.Args.Items {
				funcName := fun.Name
				var argName string
				switch inode := item.(type) {
				case *ast.ParamRef:
					if inode.Number != ref.ref.Number {
						continue
					}
				case *ast.TypeCast:
					pr, ok := inode.Arg.(*ast.ParamRef)
					if !ok {
						continue
					}
					if pr.Number != ref.ref.Number {
						continue
					}
				case *ast.NamedArgExpr:
					pr, ok := inode.Arg.(*ast.ParamRef)
					if !ok {
						continue
					}
					if pr.Number != ref.ref.Number {
						continue
					}
					if inode.Name != nil {
						argName = *inode.Name
					}
				default:
					continue
				}

				if fun.Args == nil {
					defaultName := funcName
					if argName != "" {
						defaultName = argName
					}

					defaultP := named.NewInferredParam(defaultName, false)
					p, isNamed := params.FetchMerge(ref.ref.Number, defaultP)
					a = append(a, Parameter{
						Number: ref.ref.Number,
						Column: &Column{
							Name:         p.Name(),
							DataType:     "any",
							IsNamedParam: isNamed,
							NotNull:      p.NotNull(),
						},
					})
					continue
				}

				var paramName string
				var paramType *catalog.QualName
				if argName == "" {
					paramName = fun.Args[i].Name
					paramType = fun.Args[i].Type
				} else {
					paramName = argName
					for _, arg := range fun.Args {
						if arg.Name == argName {
							paramType = arg.Type
						}
					}
					if paramType == nil {
						panic(fmt.Sprintf("named argument %s has no type", paramName))
					}
				}
				if paramName == "" {
					paramName = funcName
				}

				defaultP := named.NewInferredParam(paramName, true)
				p, isNamed := params.FetchMerge(ref.ref.Number, defaultP)
				a = append(a, Parameter{
					Number: ref.ref.Number,
					Column: &Column{
						Name:         p.Name(),
						DataType:     dataType(paramType),
						NotNull:      p.NotNull(),
						IsNamedParam: isNamed,
					},
				})
			}

			if fun.ReturnType == nil {
				continue
			}

			table, ok := c.Table(fun.ReturnType.Schema, fun.ReturnType.Name)
			if !ok {
				// The return type wasn't a table.
				continue
			}
			err = indexTable(table)
			if err != nil {
				return nil, err
			}

		case *ast.ResTarget:
			if n.Name == nil {
				return nil, fmt.Errorf("*ast.ResTarget has nil name")
			}
			key := *n.Name

			var schema, rel string
			// TODO: Deprecate defaultTable
			if defaultTable != nil {
				schema = defaultTable.Schema
				rel = defaultTable.Name
			}
			if ref.rv != nil {
				fqn, err := ParseTableName(ref.rv)
				if err != nil {
					return nil, err
				}
				schema = fqn.Schema
				rel = fqn.Name
			}
			if schema == "" {
				schema = c.DefaultSchema
			}

			tableMap, ok := typeMap[schema][rel]
			if !ok {
				return nil, sqlerr.RelationNotFound(rel)
			}

			if c, ok := tableMap[key]; ok {
				defaultP := named.NewInferredParam(key, c.IsNotNull)
				p, isNamed := params.FetchMerge(ref.ref.Number, defaultP)
				a = append(a, Parameter{
					Number: ref.ref.Number,
					Column: &Column{
						Name:         p.Name(),
						DataType:     dataType(c.Type),
						NotNull:      p.NotNull(),
						IsArray:      c.IsArray,
						Table:        &catalog.QualName{Schema: schema, Name: rel},
						Length:       c.Length,
						IsNamedParam: isNamed,
					},
				})
			} else {
				return nil, &sqlerr.Error{
					Code:     "42703",
					Message:  fmt.Sprintf("column \"%s\" does not exist", key),
					Location: n.Location,
				}
			}

		case *ast.TypeCast:
			if n.TypeName == nil {
				return nil, fmt.Errorf("*ast.TypeCast has nil type name")
			}
			col := toColumn(n.TypeName)
			defaultP := named.NewInferredParam(col.Name, col.NotNull)
			p, _ := params.FetchMerge(ref.ref.Number, defaultP)

			col.Name = p.Name()
			col.NotNull = p.NotNull()
			a = append(a, Parameter{
				Number: ref.ref.Number,
				Column: col,
			})

		case *ast.ParamRef:
			a = append(a, Parameter{Number: ref.ref.Number})

		case *ast.In:
			if n == nil || n.List == nil {
				fmt.Println("ast.In is nil")
				continue
			}

			number := 0
			if pr, ok := n.List[0].(*ast.ParamRef); ok {
				number = pr.Number
			}

			location := 0
			var key, alias string
			var items []string

			if left, ok := n.Expr.(*ast.ColumnRef); ok {
				location = left.Location
				items = stringSlice(left.Fields)
			} else if left, ok := n.Expr.(*ast.ParamRef); ok {
				if len(n.List) <= 0 {
					continue
				}
				if right, ok := n.List[0].(*ast.ColumnRef); ok {
					location = left.Location
					items = stringSlice(right.Fields)
				} else {
					continue
				}
			} else {
				continue
			}

			switch len(items) {
			case 1:
				key = items[0]
			case 2:
				alias = items[0]
				key = items[1]
			default:
				panic("too many field items: " + strconv.Itoa(len(items)))
			}

			var found int
			if n.Sel == nil {
				search := tables
				if alias != "" {
					if original, ok := aliasMap[alias]; ok {
						search = []*catalog.QualName{original}
					} else {
						for _, fqn := range tables {
							if fqn.Name == alias {
								search = []*catalog.QualName{fqn}
							}
						}
					}
				}

				for _, table := range search {
					schema := table.Schema
					if schema == "" {
						schema = c.DefaultSchema
					}
					if c, ok := typeMap[schema][table.Name][key]; ok {
						found += 1
						if ref.name != "" {
							key = ref.name
						}
						defaultP := named.NewInferredParam(key, c.IsNotNull)
						p, isNamed := params.FetchMerge(ref.ref.Number, defaultP)
						a = append(a, Parameter{
							Number: number,
							Column: &Column{
								Name:         p.Name(),
								DataType:     dataType(c.Type),
								NotNull:      c.IsNotNull,
								IsArray:      c.IsArray,
								Table:        table,
								IsNamedParam: isNamed,
							},
						})
					}
				}
			} else {
				fmt.Println("------------------------")
			}

			if found == 0 {
				return nil, &sqlerr.Error{
					Code:     "42703",
					Message:  fmt.Sprintf("396: column \"%s\" does not exist", key),
					Location: location,
				}
			}
			if found > 1 {
				return nil, &sqlerr.Error{
					Code:     "42703",
					Message:  fmt.Sprintf("in same name column reference \"%s\" is ambiguous", key),
					Location: location,
				}
			}

		default:
			fmt.Printf("unsupported reference type: %T", n)
		}
	}
	return a, nil
}