package tn_cache

import (
	kwilconfig "github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// testConfig holds all test-related configuration in one place
type testConfig struct {
	config   map[string]string
	dbConfig *kwilconfig.DBConfig
	sqlDB    sql.DB
}

var testOverrides = &testConfig{}

// SetTestConfiguration sets configuration for testing purposes
// This bypasses the normal service.LocalConfig.Extensions mechanism
func SetTestConfiguration(extConfig map[string]string) {
	testOverrides.config = extConfig
}

// SetTestDBConfiguration sets database configuration for testing
func SetTestDBConfiguration(dbConfig kwilconfig.DBConfig) {
	testOverrides.dbConfig = &dbConfig
}

// SetTestDB allows tests to inject their own database connection
// that will be used instead of creating a new pool
func SetTestDB(db sql.DB) {
	testOverrides.sqlDB = db
}

// getTestConfig returns test configuration if set
func getTestConfig() map[string]string {
	return testOverrides.config
}

// getTestDBConfig returns test DB configuration if set
func getTestDBConfig() *kwilconfig.DBConfig {
	return testOverrides.dbConfig
}

// getTestDB returns the injected test database if set
func getTestDB() sql.DB {
	return testOverrides.sqlDB
}
