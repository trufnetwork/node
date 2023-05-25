package sqlddlgenerator

import (
	"fmt"
	"strings"

	"github.com/kwilteam/kwil-db/pkg/engine/dto"
)

func indexTypeToSQLiteString(indexType dto.IndexType) (string, error) {
	err := indexType.Clean()
	if err != nil {
		return "", err
	}

	switch indexType {
	case dto.BTREE:
		return "", nil
	case dto.UNIQUE_BTREE:
		return " UNIQUE", nil
	default:
		return "", fmt.Errorf("unknown index type: %s", indexType)
	}
}

func GenerateCreateIndexStatements(tableName string, indexes []*dto.Index) ([]string, error) {
	var statements []string

	for _, index := range indexes {
		indexType, err := indexTypeToSQLiteString(index.Type)
		if err != nil {
			return nil, err
		}

		cols := make([]string, len(index.Columns))
		for i, col := range index.Columns {
			cols[i] = wrapIdent(col)
		}
		columns := strings.Join(cols, ", ")

		statement := fmt.Sprintf("CREATE%s INDEX %s ON %s (%s);", indexType, wrapIdent(index.Name), wrapIdent(tableName), columns)
		statements = append(statements, strings.TrimSpace(statement))
	}

	return statements, nil
}