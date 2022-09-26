// Code generated by sqlc-pg-gen. DO NOT EDIT.

package contrib

import (
	"github.com/kwilteam/kwil-db/internal/sqlparse/ast"
	"github.com/kwilteam/kwil-db/internal/sqlparse/catalog"
)

func PgcryptoFuncs0() []*catalog.Function {
	return []*catalog.Function{
		{
			Name: "armor",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "armor",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text[]"},
				},
				{
					Type: &ast.TypeName{Name: "text[]"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "crypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "dearmor",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "decrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "decrypt_iv",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "digest",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "digest",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "encrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "encrypt_iv",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "gen_random_bytes",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "integer"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "gen_salt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "gen_salt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "integer"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "hmac",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "hmac",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_armor_headers",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "record"},
		},
		{
			Name: "pgp_key_id",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "pgp_pub_decrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "pgp_pub_decrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "pgp_pub_decrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "pgp_pub_decrypt_bytea",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_pub_decrypt_bytea",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_pub_decrypt_bytea",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_pub_encrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_pub_encrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_pub_encrypt_bytea",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_pub_encrypt_bytea",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_sym_decrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "pgp_sym_decrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "text"},
		},
		{
			Name: "pgp_sym_decrypt_bytea",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_sym_decrypt_bytea",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_sym_encrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_sym_encrypt",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_sym_encrypt_bytea",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
		{
			Name: "pgp_sym_encrypt_bytea",
			Args: []*catalog.Argument{
				{
					Type: &ast.TypeName{Name: "bytea"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
				{
					Type: &ast.TypeName{Name: "text"},
				},
			},
			ReturnType: &ast.TypeName{Name: "bytea"},
		},
	}
}

func PgcryptoFuncs() []*catalog.Function {
	funcs := []*catalog.Function{}
	funcs = append(funcs, PgcryptoFuncs0()...)
	return funcs
}

func Pgcrypto() *catalog.Schema {
	s := &catalog.Schema{Name: "pg_catalog"}
	s.Funcs = PgcryptoFuncs()
	return s
}
