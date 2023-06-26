package sqlddlgenerator

import (
	"fmt"

	"github.com/kwilteam/kwil-db/pkg/engine/types"
)

// GenerateDDL generates the necessary table and index ddl statements for the given table
func GenerateDDL(table *types.Table) ([]string, error) {
	var statements []string

	createTableStatement, err := GenerateCreateTableStatement(table)
	if err != nil {
		return nil, err
	}
	statements = append(statements, createTableStatement)

	createIndexStatements, err := GenerateCreateIndexStatements(table.Name, table.Indexes)
	if err != nil {
		return nil, err
	}
	statements = append(statements, createIndexStatements...)

	return statements, nil
}

func wrapIdent(str string) string {
	return fmt.Sprintf(`"%s"`, str)
}