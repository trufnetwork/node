package tn_utils

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
)

// ExtensionName identifies this utilities precompile bundle.
const ExtensionName = "tn_utils"

// InitializeExtension registers the utilities precompile bundle.
func InitializeExtension() {
	if err := precompiles.RegisterInitializer(ExtensionName, initializePrecompile); err != nil {
		panic(fmt.Sprintf("failed to register %s initializer: %v", ExtensionName, err))
	}
}

func initializePrecompile(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	return buildPrecompile(), nil
}
