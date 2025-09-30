package tn_vacuum

import (
	"context"
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
	Reason          string
	DB              DBConnConfig
	PgRepackJobs    int
	PgRepackNoOrder bool
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

var mechanismFactory = func() Mechanism { return NewPgRepackMechanism() }

func newMechanism() Mechanism {
	return mechanismFactory()
}

func setMechanismFactoryForTest(f func() Mechanism) {
	mechanismFactory = f
}

func resetMechanismFactory() {
	mechanismFactory = func() Mechanism { return NewPgRepackMechanism() }
}
