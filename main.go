package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elnahomenick123/MinIO/cmd"
)

type demoProducer struct {
	id           int
	brokerOnline *bool
	mu           *sync.Mutex
}

func (p *demoProducer) Send(event cmd.Event) error {
	p.mu.Lock()
	online := *p.brokerOnline
	p.mu.Unlock()

	if !online {
		return errors.New("broker connection lost")
	}
	fmt.Printf("[Producer %d] Sent event: %s -> %s\n", p.id, event.Key, string(event.Value))
	return nil
}

func (p *demoProducer) Close() error {
	fmt.Printf("[Producer %d] Closed\n", p.id)
	return nil
}

func main() {
	fmt.Println("Starting MinIO Kafka Notification Queue Stall Fix Demo...")

	var (
		mu           sync.Mutex
		brokerOnline = true
		producerID   int32
	)

	newProducer := func(brokers []string, topic string) (cmd.KafkaProducer, error) {
		mu.Lock()
		online := brokerOnline
		mu.Unlock()

		if !online {
			return nil, errors.New("broker offline")
		}

		id := atomic.AddInt32(&producerID, 1)
		fmt.Printf("Creating new producer connection #%d to brokers %v for topic %s\n", id, brokers, topic)
		return &demoProducer{
			id:           int(id),
			brokerOnline: &brokerOnline,
			mu:           &mu,
		}, nil
	}

	target := cmd.NewKafkaTarget("demo-kafka", []string{"localhost:9092"}, "bucket-events", 10, newProducer)
	target.Start()
	defer target.Close()

	// 1. Send event while online
	fmt.Println("\n--- Step 1: Sending event while broker is online ---")
	target.Send(cmd.Event{Key: "event-1", Value: []byte("object-created-1")})
	time.Sleep(200 * time.Millisecond)

	// 2. Simulate broker offline
	fmt.Println("\n--- Step 2: Simulating broker offline ---")
	mu.Lock()
	brokerOnline = false
	mu.Unlock()

	// Send events while offline (they should queue up)
	target.Send(cmd.Event{Key: "event-2", Value: []byte("object-created-2")})
	target.Send(cmd.Event{Key: "event-3", Value: []byte("object-created-3")})
	time.Sleep(500 * time.Millisecond)

	// 3. Bring broker back online
	fmt.Println("\n--- Step 3: Simulating broker recovery ---")
	mu.Lock()
	brokerOnline = true
	mu.Unlock()

	// Wait for reconnection and queue draining
	time.Sleep(1 * time.Second)

	fmt.Println("\nDemo finished successfully!")
}
