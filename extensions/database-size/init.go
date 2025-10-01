package database_size

import (
	"fmt"

	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

// InitializeExtension registers the database_size extension for visibility in logs
func InitializeExtension() {
	err := precompiles.RegisterInitializer(ExtensionName, InitializeDatabaseSizePrecompile)
	if err != nil {
		panic(fmt.Sprintf("failed to register %s initializer: %v", ExtensionName, err))
	}
}