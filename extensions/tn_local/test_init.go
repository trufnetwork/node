package tn_local

import (
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// testOverrides holds test-injected state.
var testOverrides struct {
	sqlDB sql.DB
}

// SetTestDB allows tests to inject a database connection.
func SetTestDB(db sql.DB) {
	testOverrides.sqlDB = db
}

func getTestDB() sql.DB {
	return testOverrides.sqlDB
}
