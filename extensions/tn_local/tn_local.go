package tn_local

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	rpcserver "github.com/trufnetwork/kwil-db/node/services/jsonrpc"
)

// endBlockHook updates the cached block height after each committed block.
// This allows local stream operations to use the same created_at = block height
// semantics as consensus streams.
func endBlockHook(_ context.Context, _ *common.App, block *common.BlockContext) error {
	if block == nil {
		return nil
	}
	GetExtension().height.Store(block.Height)
	return nil
}

// InitializeExtension registers the tn_local hooks.
// Called from extensions/register.go during init().
func InitializeExtension() {
	err := hooks.RegisterEngineReadyHook("tn_local_engine_ready", engineReadyHook)
	if err != nil {
		panic(fmt.Sprintf("failed to register tn_local engine ready hook: %v", err))
	}

	err = hooks.RegisterAdminServerHook("tn_local_admin", adminServerHook)
	if err != nil {
		panic(fmt.Sprintf("failed to register tn_local admin server hook: %v", err))
	}

	err = hooks.RegisterEndBlockHook("tn_local_end_block", endBlockHook)
	if err != nil {
		panic(fmt.Sprintf("failed to register tn_local end block hook: %v", err))
	}
}

// adminServerHook registers the local storage Svc on the admin JSON-RPC server.
func adminServerHook(server *rpcserver.Server) error {
	ext := GetExtension()
	server.RegisterSvc(ext)
	return nil
}

// engineReadyHook initializes the extension's database and schema.
func engineReadyHook(ctx context.Context, app *common.App) error {
	logger := app.Service.Logger.New("tn_local")

	var localDB *LocalDB
	if testDB := getTestDB(); testDB != nil {
		localDB = NewLocalDB(testDB, logger)
	} else {
		pool, err := createIndependentConnectionPool(ctx, app.Service, logger)
		if err != nil {
			return fmt.Errorf("failed to create connection pool: %w", err)
		}

		// Close pool on any subsequent failure to prevent connection leak.
		success := false
		defer func() {
			if !success {
				pool.Close()
			}
		}()

		wrapper := NewPoolDBWrapper(pool)
		localDB = NewLocalDB(wrapper, logger)

		if err := localDB.SetupSchema(ctx); err != nil {
			return fmt.Errorf("failed to setup local schema: %w", err)
		}

		success = true
	}

	// Update existing singleton in-place to preserve the pointer registered
	// with the admin server's RegisterSvc.
	ext := GetExtension()
	ext.configure(logger, localDB.db, localDB)

	logger.Info("tn_local extension initialized")
	return nil
}

// createIndependentConnectionPool creates a dedicated connection pool for local storage.
func createIndependentConnectionPool(ctx context.Context, service *common.Service, logger log.Logger) (*pgxpool.Pool, error) {
	dbConfig := service.LocalConfig.DB

	connStr := fmt.Sprintf("host=%s port=%s user=%s database=%s sslmode=disable",
		dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.DBName)

	if dbConfig.Pass != "" {
		connStr += " password=" + dbConfig.Pass
	}

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}

	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("test connection: %w", err)
	}
	conn.Release()

	logger.Info("created independent connection pool for local storage",
		"max_conns", poolConfig.MaxConns,
		"min_conns", poolConfig.MinConns)

	return pool, nil
}
