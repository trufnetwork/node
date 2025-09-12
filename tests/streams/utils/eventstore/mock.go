// Package eventstore provides mock implementations of event stores for testing
package eventstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/trufnetwork/kwil-db/extensions/listeners"
)

// mockEventStore implements listeners.EventStore for testing
type mockEventStore struct {
	mu        sync.RWMutex
	events    chan broadcastEvent
	kvStore   map[string][]byte
	broadcast []broadcastEvent
}

type broadcastEvent struct {
	eventType string
	data      []byte
	timestamp time.Time
}

// NewMockEventStore creates a new mock event store for testing
func NewMockEventStore() listeners.EventStore {
	return &mockEventStore{
		events:    make(chan broadcastEvent, 100),
		kvStore:   make(map[string][]byte),
		broadcast: make([]broadcastEvent, 0),
	}
}

// Broadcast implements listeners.EventStore
func (m *mockEventStore) Broadcast(ctx context.Context, eventType string, data []byte) error {
	ev := broadcastEvent{
		eventType: eventType,
		data:      append([]byte(nil), data...),
		timestamp: time.Now(),
	}
	m.mu.Lock()
	m.broadcast = append(m.broadcast, ev)
	m.mu.Unlock()

	// Also send to events channel for waiting
	select {
	case m.events <- ev:
	default:
		// Channel full, skip
	}
	return nil
}

// Set implements listeners.EventKV
func (m *mockEventStore) Set(ctx context.Context, key []byte, value []byte) error {
	m.mu.Lock()
	m.kvStore[string(key)] = append([]byte(nil), value...)
	m.mu.Unlock()
	return nil
}

// Get implements listeners.EventKV
func (m *mockEventStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	m.mu.RLock()
	value, exists := m.kvStore[string(key)]
	m.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("key not found")
	}
	return append([]byte(nil), value...), nil
}

// Delete implements listeners.EventKV
func (m *mockEventStore) Delete(ctx context.Context, key []byte) error {
	m.mu.Lock()
	delete(m.kvStore, string(key))
	m.mu.Unlock()
	return nil
}

// WaitForEvent waits for a specific event type to be broadcast
func (m *mockEventStore) WaitForEvent(eventType string, timeout time.Duration) error {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Check backlog first
	m.mu.RLock()
	for _, event := range m.broadcast {
		if event.eventType == eventType {
			m.mu.RUnlock()
			return nil
		}
	}
	m.mu.RUnlock()

	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("timeout waiting for event %s", eventType)
		case ev := <-m.events:
			if ev.eventType == eventType {
				return nil
			}
		}
	}
}

// GetBroadcastEvents returns all broadcast events
func (m *mockEventStore) GetBroadcastEvents() []broadcastEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]broadcastEvent, len(m.broadcast))
	copy(out, m.broadcast)
	return out
}

// MockEventStore is a type alias for backward compatibility
type MockEventStore = mockEventStore
