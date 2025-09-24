package tn_vacuum

import (
	"context"

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
}

type RunRequest struct {
	Reason string
}

type RunReport struct {
	Mechanism string
	Status    string
}

func newMechanism() Mechanism {
	return &vacuumStubMechanism{}
}

type vacuumStubMechanism struct {
	logger log.Logger
}

func (m *vacuumStubMechanism) Name() string { return "vacuum_stub" }

func (m *vacuumStubMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	m.logger = deps.Logger.New("mechanism.vacuum_stub")
	m.logger.Info("vacuum stub prepared")
	return nil
}

func (m *vacuumStubMechanism) Run(ctx context.Context, req RunRequest) (*RunReport, error) {
	m.logger.Info("vacuum stub run", "reason", req.Reason)
	return &RunReport{Mechanism: m.Name(), Status: "ok"}, nil
}

func (m *vacuumStubMechanism) Close(ctx context.Context) error {
	m.logger.Info("vacuum stub closed")
	return nil
}
