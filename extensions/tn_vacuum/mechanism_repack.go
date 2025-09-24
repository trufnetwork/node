package tn_vacuum

import (
	"context"
	"errors"
	"fmt"
	"os/exec"

	"github.com/trufnetwork/kwil-db/core/log"
)

var ErrPgRepackUnavailable = errors.New("pg_repack binary not found in PATH")

type pgRepackMechanism struct {
	logger     log.Logger
	binaryPath string
}

func NewPgRepackMechanism() Mechanism {
	return &pgRepackMechanism{}
}

func (m *pgRepackMechanism) Name() string { return "pg_repack" }

func (m *pgRepackMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	m.logger = deps.Logger.New("mechanism.pg_repack")
	path, err := exec.LookPath("pg_repack")
	if err != nil {
		m.logger.Warn("pg_repack binary not found; vacuum runs will fail until available", "error", err)
		return ErrPgRepackUnavailable
	}
	m.binaryPath = path
	m.logger.Info("pg_repack binary detected", "path", path)
	return nil
}

func (m *pgRepackMechanism) Run(ctx context.Context, req RunRequest) (*RunReport, error) {
	if m.binaryPath == "" {
		return nil, fmt.Errorf("pg_repack unavailable: %w", ErrPgRepackUnavailable)
	}
	m.logger.Info("pg_repack stub run", "reason", req.Reason, "binary", m.binaryPath)
	return &RunReport{Mechanism: m.Name(), Status: "ok"}, nil
}

func (m *pgRepackMechanism) Close(ctx context.Context) error {
	if m.logger != nil {
		m.logger.Info("pg_repack mechanism closed")
	}
	return nil
}
