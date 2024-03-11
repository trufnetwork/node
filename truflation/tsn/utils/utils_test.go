package utils

import (
	"testing"

	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"github.com/kwilteam/kwil-db/internal/engine/types"
)

func TestGetDBIDFromPath(t *testing.T) {
	tests := []struct {
		name          string
		ctx           *execution.DeploymentContext
		pathOrDBID    string
		expectedDBID  string
		expectedError bool
	}{
		{
			name:          "DBIDWithoutSlash",
			ctx:           nil,
			pathOrDBID:    "dbwithoutslash",
			expectedDBID:  "dbwithoutslash",
			expectedError: false,
		},
		{
			name:          "DBIDWithLeadingSlash",
			ctx:           &execution.DeploymentContext{Schema: &types.Schema{Owner: []byte("owner1")}},
			pathOrDBID:    "/dbname",
			expectedDBID:  utils.GenerateDBID("dbname", []byte("owner1")),
			expectedError: false,
		},
		{
			name:          "DBIDWithSlashAndNoContext",
			ctx:           nil,
			pathOrDBID:    "wallet/dbname",
			expectedDBID:  utils.GenerateDBID("dbname", []byte("wallet")),
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbID, err := GetDBIDFromPath(tt.ctx, tt.pathOrDBID)
			if err != nil && !tt.expectedError {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.expectedError && err == nil {
				t.Fatal("expected an error but got nil")
			}
			if dbID != tt.expectedDBID {
				t.Errorf("DBID mismatch - want: %v, got: %v", tt.expectedDBID, dbID)
			}
		})
	}
}
