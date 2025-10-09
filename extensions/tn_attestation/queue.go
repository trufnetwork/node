package tn_attestation

import "sync"

const (
	// MaxQueueSize is the maximum number of attestation hashes that can be queued.
	// This prevents unbounded memory growth before the signing worker (Issue 6) is implemented.
	// With deduplication, this limit applies to unique hashes only.
	MaxQueueSize = 10000
)

// AttestationQueue is a thread-safe queue for managing attestation hashes that need signing.
// It is shared between the queue_for_signing() precompile and the leader signing worker.
// The queue has a maximum size to prevent unbounded memory growth.
type AttestationQueue struct {
	mu     sync.RWMutex
	hashes map[string]struct{} // Using map for O(1) deduplication
	order  []string            // Maintains FIFO order for eviction
}

// NewAttestationQueue creates a new attestation queue.
func NewAttestationQueue() *AttestationQueue {
	return &AttestationQueue{
		hashes: make(map[string]struct{}),
		order:  make([]string, 0),
	}
}

// Enqueue adds an attestation hash to the queue if it doesn't already exist.
// Returns true if the hash was added, false if it already existed.
// If the queue is at max capacity, the oldest hash is evicted (FIFO).
func (q *AttestationQueue) Enqueue(hash string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if hash already exists
	if _, exists := q.hashes[hash]; exists {
		return false
	}

	// If at max capacity, evict the oldest hash
	if len(q.hashes) >= MaxQueueSize {
		if len(q.order) > 0 {
			oldestHash := q.order[0]
			delete(q.hashes, oldestHash)
			q.order = q.order[1:]
		}
	}

	// Add new hash
	q.hashes[hash] = struct{}{}
	q.order = append(q.order, hash)
	return true
}

// DequeueAll removes and returns all attestation hashes from the queue.
// The hashes are returned in FIFO order.
func (q *AttestationQueue) DequeueAll() []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.hashes) == 0 {
		return nil
	}

	// Return copy of order slice
	hashes := make([]string, len(q.order))
	copy(hashes, q.order)

	// Clear the queue
	q.hashes = make(map[string]struct{})
	q.order = make([]string, 0)

	return hashes
}

// Len returns the current number of hashes in the queue.
func (q *AttestationQueue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.hashes)
}

// Clear removes all hashes from the queue.
func (q *AttestationQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.hashes = make(map[string]struct{})
	q.order = make([]string, 0)
}

// Copy creates a deep copy of the queue.
func (q *AttestationQueue) Copy() *AttestationQueue {
	q.mu.RLock()
	defer q.mu.RUnlock()

	newQueue := NewAttestationQueue()
	// Copy hashes map
	for hash := range q.hashes {
		newQueue.hashes[hash] = struct{}{}
	}
	// Copy order slice
	newQueue.order = make([]string, len(q.order))
	copy(newQueue.order, q.order)
	return newQueue
}

// attestationQueueSingleton is the global queue shared between precompile and extension.
// This will be accessed by both queue_for_signing() precompile and the leader signing worker.
var attestationQueueSingleton *AttestationQueue
var queueOnce sync.Once

// GetAttestationQueue returns the global attestation queue singleton.
// This function is exported so both the precompile and leader worker can access it.
func GetAttestationQueue() *AttestationQueue {
	queueOnce.Do(func() {
		attestationQueueSingleton = NewAttestationQueue()
	})
	return attestationQueueSingleton
}
