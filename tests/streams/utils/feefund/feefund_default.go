//go:build !kwiltest

// Package feefund — non-kwiltest stub. The actual funding implementation lives
// in feefund_kwiltest.go and depends on `//go:build kwiltest` test shims in
// kwil-db's erc20-bridge package; outside that build tag, EnsureWalletFunded is
// a no-op so callers (setup, procedure, internal/benchmark) compile.
package feefund

import (
	"context"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

// Constants kept identical to feefund_kwiltest.go so callers compile uniformly.
const (
	PerStreamWei      = "6000000000000000000"  // 6 TRUF
	AttestationFeeWei = "40000000000000000000" // 40 TRUF
)

// EnsureWalletFunded is a no-op outside the kwiltest build. Non-kwiltest callers
// (e.g. internal/benchmark) must arrange ERC20 balance themselves once
// universal write fees are enforced.
func EnsureWalletFunded(ctx context.Context, platform *kwilTesting.Platform, wallet, amountWei string) error {
	return nil
}
