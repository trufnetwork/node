package tn_attestation

import "sync"

// AttestationQueue is a thread-safe queue for managing attestation hashes that need signing.
// It is shared between the queue_for_signing() precompile and the leader signing worker.
type AttestationQueue struct {
	mu     sync.RWMutex
	hashes map[string]struct{} // Using map for O(1) deduplication
}

// NewAttestationQueue creates a new attestation queue.
func NewAttestationQueue() *AttestationQueue {
	return &AttestationQueue{
		hashes: make(map[string]struct{}),
	}
}

// Enqueue adds an attestation hash to the queue if it doesn't already exist.
// Returns true if the hash was added, false if it already existed.
func (q *AttestationQueue) Enqueue(hash string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.hashes[hash]; exists {
		return false
	}

	q.hashes[hash] = struct{}{}
	return true
}

// DequeueAll removes and returns all attestation hashes from the queue.
func (q *AttestationQueue) DequeueAll() []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.hashes) == 0 {
		return nil
	}

	hashes := make([]string, 0, len(q.hashes))
	for hash := range q.hashes {
		hashes = append(hashes, hash)
	}

	// Clear the queue
	q.hashes = make(map[string]struct{})

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
}

// Copy creates a deep copy of the queue.
func (q *AttestationQueue) Copy() *AttestationQueue {
	q.mu.RLock()
	defer q.mu.RUnlock()

	newQueue := NewAttestationQueue()
	for hash := range q.hashes {
		newQueue.hashes[hash] = struct{}{}
	}
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
