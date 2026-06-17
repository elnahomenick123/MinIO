package cmd

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockProducer struct {
	sendFunc  func(event Event) error
	closeFunc func() error
}

func (m *mockProducer) Send(event Event) error {
	if m.sendFunc != nil {
		return m.sendFunc(event)
	}
	return nil
}

func (m *mockProducer) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestKafkaTarget_ReconnectionAndRecovery(t *testing.T) {
	var (
		mu              sync.Mutex
		brokerOnline    = true
		successfulSends int32
	)

	newMockProducer := func(brokers []string, topic string) (KafkaProducer, error) {
		mu.Lock()
		online := brokerOnline
		mu.Unlock()

		if !online {
			return nil, errors.New("broker offline")
		}

		return &mockProducer{
			sendFunc: func(event Event) error {
				mu.Lock()
				online := brokerOnline
				mu.Unlock()

				if !online {
					return errors.New("broker connection lost")
				}
				atomic.AddInt32(&successfulSends, 1)
				return nil
			},
			closeFunc: func() error {
				return nil
			},
		}, nil
	}

	target := NewKafkaTarget("test-kafka", []string{"localhost:9092"}, "test-topic", 10, newMockProducer)
	target.logFunc = func(format string, v ...interface{}) {}

	target.Start()
	defer target.Close()

	// 1. Send an event while broker is online
	err := target.Send(Event{Key: "key1", Value: []byte("val1")})
	if err != nil {
		t.Fatalf("Failed to send event: %v", err)
	}

	// Wait for the event to be processed
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&successfulSends) != 1 {
		t.Errorf("Expected 1 successful send, got %d", atomic.LoadInt32(&successfulSends))
	}

	// 2. Simulate broker offline
	mu.Lock()
	brokerOnline = false
	mu.Unlock()

	// Send another event
	err = target.Send(Event{Key: "key2", Value: []byte("val2")})
	if err != nil {
		t.Fatalf("Failed to send event: %v", err)
	}

	// Wait a bit to let it fail and trigger reconnection
	time.Sleep(200 * time.Millisecond)

	// 3. Bring broker back online
	mu.Lock()
	brokerOnline = true
	mu.Unlock()

	// Wait for reconnection and processing of the queued event
	time.Sleep(500 * time.Millisecond)

	if atomic.LoadInt32(&successfulSends) != 2 {
		t.Errorf("Expected 2 successful sends after recovery, got %d", atomic.LoadInt32(&successfulSends))
	}
}
