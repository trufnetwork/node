// Package eventstore provides mock implementations of event stores for testing
package eventstore

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/extensions/listeners"
)

// mockEventStore implements listeners.EventStore for testing
type mockEventStore struct {
	events    chan []byte
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
		events:    make(chan []byte, 100),
		kvStore:   make(map[string][]byte),
		broadcast: make([]broadcastEvent, 0),
	}
}

// Broadcast implements listeners.EventStore
func (m *mockEventStore) Broadcast(ctx context.Context, eventType string, data []byte) error {
	m.broadcast = append(m.broadcast, broadcastEvent{
		eventType: eventType,
		data:      data,
		timestamp: time.Now(),
	})

	// Also send to events channel for waiting
	select {
	case m.events <- data:
	default:
		// Channel full, skip
	}

	return nil
}

// Set implements listeners.EventKV
func (m *mockEventStore) Set(ctx context.Context, key []byte, value []byte) error {
	m.kvStore[string(key)] = value
	return nil
}

// Get implements listeners.EventKV
func (m *mockEventStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, exists := m.kvStore[string(key)]
	if !exists {
		return nil, fmt.Errorf("key not found")
	}
	return value, nil
}

// Delete implements listeners.EventKV
func (m *mockEventStore) Delete(ctx context.Context, key []byte) error {
	delete(m.kvStore, string(key))
	return nil
}

// WaitForEvent waits for a specific event type to be broadcast
func (m *mockEventStore) WaitForEvent(eventType string, timeout time.Duration) error {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("timeout waiting for event %s", eventType)
		default:
			// Check if event was already broadcast
			for _, event := range m.broadcast {
				if event.eventType == eventType {
					return nil
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// GetBroadcastEvents returns all broadcast events
func (m *mockEventStore) GetBroadcastEvents() []broadcastEvent {
	return m.broadcast
}

// MockEventStore is a type alias for backward compatibility
type MockEventStore = mockEventStore
