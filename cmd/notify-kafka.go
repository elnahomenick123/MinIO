package cmd

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Event represents a notification event.
type Event struct {
	Key   string
	Value []byte
}

// KafkaProducer defines the interface for sending messages to Kafka.
type KafkaProducer interface {
	Send(event Event) error
	Close() error
}

// Target status constants
const (
	statusDisconnected int32 = iota
	statusConnected
	statusReconnecting
)

// KafkaTarget manages the connection to Kafka and the event queue.
type KafkaTarget struct {
	id          string
	brokers     []string
	topic       string
	queue       chan Event
	maxQueue    int
	newProducer func(brokers []string, topic string) (KafkaProducer, error)

	mu       sync.RWMutex
	producer KafkaProducer
	status   int32
	closed   int32

	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc

	logFunc func(format string, v ...interface{})
}

// NewKafkaTarget creates a new KafkaTarget.
func NewKafkaTarget(id string, brokers []string, topic string, maxQueue int, newProducer func([]string, string) (KafkaProducer, error)) *KafkaTarget {
	ctx, cancel := context.WithCancel(context.Background())
	t := &KafkaTarget{
		id:          id,
		brokers:     brokers,
		topic:       topic,
		queue:       make(chan Event, maxQueue),
		maxQueue:    maxQueue,
		newProducer: newProducer,
		status:      statusDisconnected,
		ctx:         ctx,
		cancelFunc:  cancel,
		logFunc:     log.Printf,
	}
	return t
}

// Start starts the event dispatcher loop.
func (t *KafkaTarget) Start() {
	t.wg.Add(1)
	go t.dispatcher()
	t.triggerReconnection()
}

// Close closes the target and stops the dispatcher.
func (t *KafkaTarget) Close() error {
	if !atomic.CompareAndSwapInt32(&t.closed, 0, 1) {
		return nil
	}
	t.cancelFunc()
	t.wg.Wait()

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.producer != nil {
		t.producer.Close()
		t.producer = nil
	}
	t.status = statusDisconnected
	return nil
}

// Send adds an event to the queue.
func (t *KafkaTarget) Send(event Event) error {
	if atomic.LoadInt32(&t.closed) == 1 {
		return errors.New("target is closed")
	}

	select {
	case t.queue <- event:
		return nil
	case <-t.ctx.Done():
		return errors.New("target is closed")
	}
}

// dispatcher drains the queue and sends events to Kafka.
func (t *KafkaTarget) dispatcher() {
	defer t.wg.Done()

	for {
		select {
		case <-t.ctx.Done():
			return
		case event, ok := <-t.queue:
			if !ok {
				return
			}
			t.sendWithRetry(event)
		}
	}
}

func (t *KafkaTarget) sendWithRetry(event Event) {
	for {
		if atomic.LoadInt32(&t.closed) == 1 {
			return
		}

		t.mu.RLock()
		prod := t.producer
		status := atomic.LoadInt32(&t.status)
		t.mu.RUnlock()

		if status != statusConnected || prod == nil {
			t.triggerReconnection()
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}

		err := prod.Send(event)
		if err != nil {
			t.logFunc("Kafka target [%s] failed to send event: %v. Triggering reconnection.", t.id, err)
			t.handleFailure()
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}

		return
	}
}

func (t *KafkaTarget) handleFailure() {
	t.mu.Lock()
	if t.producer != nil {
		t.producer.Close()
		t.producer = nil
	}
	atomic.StoreInt32(&t.status, statusDisconnected)
	t.mu.Unlock()

	t.triggerReconnection()
}

func (t *KafkaTarget) triggerReconnection() {
	if atomic.LoadInt32(&t.closed) == 1 {
		return
	}

	if !atomic.CompareAndSwapInt32(&t.status, statusDisconnected, statusReconnecting) {
		return
	}

	t.wg.Add(1)
	go t.reconnectLoop()
}

func (t *KafkaTarget) reconnectLoop() {
	defer t.wg.Done()

	backoff := 100 * time.Millisecond
	maxBackoff := 5 * time.Second

	t.logFunc("Kafka target [%s] starting reconnection loop.", t.id)

	for {
		if atomic.LoadInt32(&t.closed) == 1 {
			return
		}

		prod, err := t.newProducer(t.brokers, t.topic)
		if err == nil {
			t.mu.Lock()
			if atomic.LoadInt32(&t.closed) == 1 {
				if prod != nil {
					prod.Close()
				}
				t.mu.Unlock()
				return
			}
			t.producer = prod
			atomic.StoreInt32(&t.status, statusConnected)
			t.logFunc("Kafka target [%s] successfully connected/reconnected.", t.id)
			t.mu.Unlock()
			return
		}

		t.logFunc("Kafka target [%s] connection attempt failed: %v. Retrying in %v...", t.id, err, backoff)

		select {
		case <-t.ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}
