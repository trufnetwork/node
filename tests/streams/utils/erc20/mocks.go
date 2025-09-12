// Package erc20 provides mock implementations for ERC-20 bridge testing
package erc20

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/listeners"
)

// CreateMockERC20Transfer creates a mock ERC-20 transfer event
func CreateMockERC20Transfer(from, to, amount string) []byte {
	// This would create a properly formatted Ethereum log event
	// For now, return a simple JSON representation
	return []byte(fmt.Sprintf(`{"from":"%s","to":"%s","amount":"%s"}`, from, to, amount))
}

// CreateMockERC20Deposit creates a mock deposit event for the bridge
func CreateMockERC20Deposit(user, token, amount string) []byte {
	return []byte(fmt.Sprintf(`{"user":"%s","token":"%s","amount":"%s","type":"deposit"}`, user, token, amount))
}

// CreateMockERC20Withdrawal creates a mock withdrawal event for the bridge
func CreateMockERC20Withdrawal(user, token, amount string) []byte {
	return []byte(fmt.Sprintf(`{"user":"%s","token":"%s","amount":"%s","type":"withdrawal"}`, user, token, amount))
}

// MockERC20Listener creates a mock listener for testing ERC-20 bridge functionality
func MockERC20Listener(expectedEvents []string) listeners.ListenFunc {
	return func(ctx context.Context, service *common.Service, eventstore listeners.EventStore) error {
		// Simulate receiving events
		for _, eventType := range expectedEvents {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				mockData := CreateMockERC20Transfer("0x123", "0x456", "1000000000000000000")
				if err := eventstore.Broadcast(ctx, eventType, mockData); err != nil {
					return fmt.Errorf("failed to broadcast event: %w", err)
				}
			}
		}

		// Wait for context cancellation
		<-ctx.Done()
		return ctx.Err()
	}
}

// MockERC20DepositListener creates a mock listener that simulates deposit events
func MockERC20DepositListener(userAddress, tokenAddress, amount string) listeners.ListenFunc {
	return func(ctx context.Context, service *common.Service, eventstore listeners.EventStore) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			mockData := CreateMockERC20Deposit(userAddress, tokenAddress, amount)
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
			mockData := CreateMockERC20Withdrawal(userAddress, tokenAddress, amount)
			if err := eventstore.Broadcast(ctx, "erc20_withdrawal", mockData); err != nil {
				return fmt.Errorf("failed to broadcast withdrawal event: %w", err)
			}
		}

		// Wait for context cancellation
		<-ctx.Done()
		return ctx.Err()
	}
}
