package tn_attestation

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

// ensurePrecompileRegistered ensures the precompile is registered before use.
// This prevents panics when running subtests in isolation.
func ensurePrecompileRegistered(t *testing.T) {
	t.Helper()
	if _, ok := precompiles.RegisteredPrecompiles()[ExtensionName]; !ok {
		require.NoError(t, registerPrecompile())
	}
}

// getQueueForSigningHandler retrieves the queue_for_signing handler by name.
// This is more robust than indexing Methods[0].
func getQueueForSigningHandler(t *testing.T, precompile precompiles.Precompile) func(*common.EngineContext, *common.App, []any, func([]any) error) error {
	t.Helper()
	for _, method := range precompile.Methods {
		if method.Name == "queue_for_signing" {
			return method.Handler
		}
	}
	t.Fatal("queue_for_signing method not found")
	return nil
}

func TestAttestationQueue(t *testing.T) {
	t.Run("NewQueue", func(t *testing.T) {
		q := NewAttestationQueue()
		assert.NotNil(t, q)
		assert.Equal(t, 0, q.Len())
	})

	t.Run("Enqueue", func(t *testing.T) {
		q := NewAttestationQueue()

		// First enqueue should succeed
		added := q.Enqueue("hash1")
		assert.True(t, added)
		assert.Equal(t, 1, q.Len())

		// Duplicate enqueue should fail
		added = q.Enqueue("hash1")
		assert.False(t, added)
		assert.Equal(t, 1, q.Len())

		// Different hash should succeed
		added = q.Enqueue("hash2")
		assert.True(t, added)
		assert.Equal(t, 2, q.Len())
	})

	t.Run("DequeueAll", func(t *testing.T) {
		q := NewAttestationQueue()
		q.Enqueue("hash1")
		q.Enqueue("hash2")
		q.Enqueue("hash3")

		hashes := q.DequeueAll()
		assert.Len(t, hashes, 3)
		assert.Contains(t, hashes, "hash1")
		assert.Contains(t, hashes, "hash2")
		assert.Contains(t, hashes, "hash3")

		// Queue should be empty after dequeue all
		assert.Equal(t, 0, q.Len())

		// Dequeuing empty queue should return nil
		hashes = q.DequeueAll()
		assert.Nil(t, hashes)
	})

	t.Run("Clear", func(t *testing.T) {
		q := NewAttestationQueue()
		q.Enqueue("hash1")
		q.Enqueue("hash2")
		assert.Equal(t, 2, q.Len())

		q.Clear()
		assert.Equal(t, 0, q.Len())
	})

	t.Run("Copy", func(t *testing.T) {
		q1 := NewAttestationQueue()
		q1.Enqueue("hash1")
		q1.Enqueue("hash2")

		q2 := q1.Copy()
		assert.Equal(t, q1.Len(), q2.Len())

		// Modifying copy shouldn't affect original
		q2.Enqueue("hash3")
		assert.Equal(t, 2, q1.Len())
		assert.Equal(t, 3, q2.Len())
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		q := NewAttestationQueue()
		var wg sync.WaitGroup

		// Multiple goroutines enqueueing
		wg.Add(10)
		for i := range 10 {
			go func(id int) {
				defer wg.Done()
				for range 100 {
					q.Enqueue(string(rune('a' + id)))
				}
			}(i)
		}

		// Wait for all goroutines
		wg.Wait()

		// Should have exactly 10 unique hashes (one per goroutine)
		assert.Equal(t, 10, q.Len())
	})

	t.Run("MaxQueueSize", func(t *testing.T) {
		q := NewAttestationQueue()

		// Fill queue to max capacity
		for i := range MaxQueueSize {
			added := q.Enqueue(fmt.Sprintf("hash_%d", i))
			assert.True(t, added)
		}
		assert.Equal(t, MaxQueueSize, q.Len())

		// Adding one more should evict the oldest (hash_0)
		added := q.Enqueue("hash_new")
		assert.True(t, added)
		assert.Equal(t, MaxQueueSize, q.Len())

		// Verify FIFO eviction: hash_0 should be gone
		hashes := q.DequeueAll()
		assert.NotContains(t, hashes, "hash_0", "oldest hash should have been evicted")
		assert.Contains(t, hashes, "hash_new", "new hash should be present")
		assert.Contains(t, hashes, "hash_1", "second-oldest should still be present")
	})

	t.Run("FIFOOrderPreserved", func(t *testing.T) {
		q := NewAttestationQueue()

		// Enqueue in order
		q.Enqueue("first")
		q.Enqueue("second")
		q.Enqueue("third")

		// DequeueAll should return in FIFO order
		hashes := q.DequeueAll()
		assert.Equal(t, []string{"first", "second", "third"}, hashes)
	})
}

func TestQueueForSigningPrecompile(t *testing.T) {
	// Reset singleton for test isolation
	queueOnce = sync.Once{}
	attestationQueueSingleton = nil

	t.Run("RegistrationSuccess", func(t *testing.T) {
		// Register the precompile
		err := registerPrecompile()
		require.NoError(t, err)

		registered := precompiles.RegisteredPrecompiles()
		require.Contains(t, registered, ExtensionName)
	})

	t.Run("LeaderQueuesHash", func(t *testing.T) {
		ensurePrecompileRegistered(t)

		// Reset queue
		queue := GetAttestationQueue()
		queue.Clear()

		// Create mock leader identity
		leaderPriv, leaderPub, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		_ = leaderPriv // unused

		// Create context where node IS the leader
		ctx := &common.EngineContext{
			TxContext: &common.TxContext{
				Ctx: context.Background(),
				BlockContext: &common.BlockContext{
					Proposer: leaderPub,
				},
			},
		}

		app := &common.App{
			Service: &common.Service{
				Logger:   log.DiscardLogger,
				Identity: leaderPub.Bytes(),
			},
		}

		// Initialize the precompile
		initializer := precompiles.RegisteredPrecompiles()[ExtensionName]
		precompile, err := initializer(context.Background(), app.Service, nil, "", nil)
		require.NoError(t, err)

		// Call queue_for_signing
		handler := getQueueForSigningHandler(t, precompile)
		err = handler(ctx, app, []any{"test_hash_123"}, func([]any) error { return nil })
		require.NoError(t, err)

		// Verify hash was queued
		assert.Equal(t, 1, queue.Len())
		hashes := queue.DequeueAll()
		assert.Contains(t, hashes, "test_hash_123")
	})

	t.Run("NonLeaderDoesNotQueue", func(t *testing.T) {
		ensurePrecompileRegistered(t)

		// Reset queue
		queue := GetAttestationQueue()
		queue.Clear()

		// Create different keys for leader and validator
		leaderPriv, leaderPub, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		_ = leaderPriv

		validatorPriv, validatorPub, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		_ = validatorPriv

		// Create context where node is NOT the leader
		ctx := &common.EngineContext{
			TxContext: &common.TxContext{
				Ctx: context.Background(),
				BlockContext: &common.BlockContext{
					Proposer: leaderPub, // Different from validator's identity
				},
			},
		}

		app := &common.App{
			Service: &common.Service{
				Logger:   log.DiscardLogger,
				Identity: validatorPub.Bytes(), // Validator's identity
			},
		}

		// Initialize the precompile
		initializer := precompiles.RegisteredPrecompiles()[ExtensionName]
		precompile, err := initializer(context.Background(), app.Service, nil, "", nil)
		require.NoError(t, err)

		// Call queue_for_signing
		handler := getQueueForSigningHandler(t, precompile)
		err = handler(ctx, app, []any{"test_hash_456"}, func([]any) error { return nil })
		require.NoError(t, err)

		// Verify hash was NOT queued (non-leader is no-op)
		assert.Equal(t, 0, queue.Len())
	})

	t.Run("NoProposerIsNoOp", func(t *testing.T) {
		ensurePrecompileRegistered(t)

		// Reset queue
		queue := GetAttestationQueue()
		queue.Clear()

		// Create context with no proposer
		ctx := &common.EngineContext{
			TxContext: &common.TxContext{
				Ctx: context.Background(),
				BlockContext: &common.BlockContext{
					Proposer: nil, // No proposer
				},
			},
		}

		app := &common.App{
			Service: &common.Service{
				Logger:   log.DiscardLogger,
				Identity: []byte("some_identity"),
			},
		}

		// Initialize the precompile
		initializer := precompiles.RegisteredPrecompiles()[ExtensionName]
		precompile, err := initializer(context.Background(), app.Service, nil, "", nil)
		require.NoError(t, err)

		// Call queue_for_signing
		handler := getQueueForSigningHandler(t, precompile)
		err = handler(ctx, app, []any{"test_hash_789"}, func([]any) error { return nil })
		require.NoError(t, err)

		// Verify hash was NOT queued
		assert.Equal(t, 0, queue.Len())
	})

	t.Run("PreservesDeterminism", func(t *testing.T) {
		ensurePrecompileRegistered(t)

		// Reset queue
		queue := GetAttestationQueue()
		queue.Clear()

		leaderPriv, leaderPub, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		_ = leaderPriv

		// Call from leader
		ctxLeader := &common.EngineContext{
			TxContext: &common.TxContext{
				Ctx: context.Background(),
				BlockContext: &common.BlockContext{
					Proposer: leaderPub,
				},
			},
		}

		appLeader := &common.App{
			Service: &common.Service{
				Logger:   log.DiscardLogger,
				Identity: leaderPub.Bytes(),
			},
		}

		validatorPriv, validatorPub, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		_ = validatorPriv

		// Call from non-leader
		ctxValidator := &common.EngineContext{
			TxContext: &common.TxContext{
				Ctx: context.Background(),
				BlockContext: &common.BlockContext{
					Proposer: leaderPub,
				},
			},
		}

		appValidator := &common.App{
			Service: &common.Service{
				Logger:   log.DiscardLogger,
				Identity: validatorPub.Bytes(),
			},
		}

		// Initialize the precompile
		initializer := precompiles.RegisteredPrecompiles()[ExtensionName]
		precompile, err := initializer(context.Background(), appLeader.Service, nil, "", nil)
		require.NoError(t, err)

		handler := getQueueForSigningHandler(t, precompile)

		// Both should return nil error (deterministic)
		err1 := handler(ctxLeader, appLeader, []any{"test_hash"}, func([]any) error { return nil })
		err2 := handler(ctxValidator, appValidator, []any{"test_hash"}, func([]any) error { return nil })

		assert.NoError(t, err1)
		assert.NoError(t, err2)

		// Both return the same error status (deterministic)
		// But only leader's queue is affected (non-deterministic side effect)
		assert.Equal(t, 1, queue.Len())
	})

	t.Run("EmptyHashReturnsError", func(t *testing.T) {
		ensurePrecompileRegistered(t)

		// Create mock leader identity
		leaderPriv, leaderPub, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		_ = leaderPriv

		// Create context where node IS the leader
		ctx := &common.EngineContext{
			TxContext: &common.TxContext{
				Ctx: context.Background(),
				BlockContext: &common.BlockContext{
					Proposer: leaderPub,
				},
			},
		}

		app := &common.App{
			Service: &common.Service{
				Logger:   log.DiscardLogger,
				Identity: leaderPub.Bytes(),
			},
		}

		// Initialize the precompile
		initializer := precompiles.RegisteredPrecompiles()[ExtensionName]
		precompile, err := initializer(context.Background(), app.Service, nil, "", nil)
		require.NoError(t, err)

		// Call queue_for_signing with empty hash
		handler := getQueueForSigningHandler(t, precompile)
		err = handler(ctx, app, []any{""}, func([]any) error { return nil })

		// Should return error for empty hash
		require.Error(t, err)
		assert.Contains(t, err.Error(), "attestation_hash cannot be empty")
	})
}

func TestEngineReadyHook(t *testing.T) {
	t.Run("NilAppHandling", func(t *testing.T) {
		// engineReadyHook should handle nil app gracefully
		err := engineReadyHook(context.Background(), nil)
		assert.NoError(t, err, "should handle nil app without error")
	})

	t.Run("NilServiceHandling", func(t *testing.T) {
		// engineReadyHook should handle nil service gracefully
		app := &common.App{
			Service: nil,
		}
		err := engineReadyHook(context.Background(), app)
		assert.NoError(t, err, "should handle nil service without error")
	})

	t.Run("NoRootDirHandling", func(t *testing.T) {
		// Reset signer for test isolation
		ResetValidatorSignerForTesting()

		// When no root dir is available, should not panic and log warning
		app := &common.App{
			Service: &common.Service{
				Logger: log.DiscardLogger,
			},
		}
		err := engineReadyHook(context.Background(), app)
		assert.NoError(t, err, "should handle missing root dir without error")

		// Signer should not be initialized
		signer := GetValidatorSigner()
		assert.Nil(t, signer, "signer should not be initialized without key file")
	})

	// Note: Full integration test with actual key loading is deferred to Issue #1209
	// where it will be tested as part of end-to-end leader signing workflow
}
