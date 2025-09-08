// Package testutils provides ERC-20 bridge testing utilities.
//
// This file contains test-specific utilities for ERC-20 bridge testing.
// All core functionality is implemented in utils.go for better organization.
//
// Key features:
// - Mock ERC-20 event generation
// - Test-specific listener implementations
// - ERC-20 bridge testing helpers
package testutils

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/listeners"
	"github.com/trufnetwork/node/tests/streams/utils/erc20"
)

// Note: Core mock functions are defined in utils.go for better organization
// These functions are available via the testutils package

// MockERC20DepositListener creates a mock listener that simulates deposit events
func MockERC20DepositListener(userAddress, tokenAddress, amount string) listeners.ListenFunc {
	return func(ctx context.Context, service *common.Service, eventstore listeners.EventStore) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			mockData := erc20.CreateMockERC20Deposit(userAddress, tokenAddress, amount)
			if err := eventstore.Broadcast(ctx, "erc20_deposit", mockData); err != nil {
				return fmt.Errorf("failed to broadcast deposit event: %w", err)
			}
		}

		// Wait for context cancellation
		<-ctx.Done()
		return ctx.Err()
	}
}

// MockERC20WithdrawalListener creates a mock listener that simulates withdrawal events
func MockERC20WithdrawalListener(userAddress, tokenAddress, amount string) listeners.ListenFunc {
	return func(ctx context.Context, service *common.Service, eventstore listeners.EventStore) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			mockData := erc20.CreateMockERC20Withdrawal(userAddress, tokenAddress, amount)
			if err := eventstore.Broadcast(ctx, "erc20_withdrawal", mockData); err != nil {
				return fmt.Errorf("failed to broadcast withdrawal event: %w", err)
			}
		}

		// Wait for context cancellation
		<-ctx.Done()
		return ctx.Err()
	}
}
