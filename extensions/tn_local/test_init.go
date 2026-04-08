package tn_local

import (
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// testOverrides holds test-injected state.
var testOverrides struct {
	sqlDB       sql.DB
	nodeAddress string
}

// SetTestDB allows tests to inject a database connection.
func SetTestDB(db sql.DB) {
	testOverrides.sqlDB = db
}

func getTestDB() sql.DB {
	return testOverrides.sqlDB
}

// SetTestNodeAddress allows tests to inject a node operator address without
// setting up a real ValidatorSigner. Pass "" to clear.
func SetTestNodeAddress(addr string) {
	testOverrides.nodeAddress = addr
}

func getTestNodeAddress() string {
	return testOverrides.nodeAddress
}
