package tn_vacuum

import (
	"context"
	"sync"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
)

type Mechanism interface {
	Name() string
	Prepare(ctx context.Context, deps MechanismDeps) error
	Run(ctx context.Context, req RunRequest) (*RunReport, error)
	Close(ctx context.Context) error
}

type MechanismDeps struct {
	Logger log.Logger
	DB     DBConnConfig
}

type RunRequest struct {
	Reason       string
	DB           DBConnConfig
	PgRepackJobs int
}

type RunReport struct {
	Mechanism       string
	Status          string
	Duration        time.Duration
	TablesProcessed int
	Error           string
}

type DBConnConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

var (
	mechanismFactory   = func() Mechanism { return NewPgRepackMechanism() }
	mechanismFactoryMu sync.RWMutex
)

func newMechanism() Mechanism {
	mechanismFactoryMu.RLock()
	defer mechanismFactoryMu.RUnlock()
	return mechanismFactory()
}

func setMechanismFactoryForTest(f func() Mechanism) {
	mechanismFactoryMu.Lock()
	defer mechanismFactoryMu.Unlock()
	mechanismFactory = f
}

func resetMechanismFactory() {
	mechanismFactoryMu.Lock()
	defer mechanismFactoryMu.Unlock()
	mechanismFactory = func() Mechanism { return NewPgRepackMechanism() }
}
