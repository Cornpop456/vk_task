package subpub

import (
	"context"
	"errors"
	"log"
	"sync"
)

const (
	queueSize = 100
)

type MessageHandler func(msg interface{})

type Subscription interface {
	Unsubscribe()
}

type SubPub interface {
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	Publish(subject string, msg interface{}) error
	Close(ctx context.Context) error
	GetLenQueue() int
}

var ErrBusClosed = errors.New("event bus is closed")

func NewSubPub() SubPub {
	return NewEventBus()
}

type SubscriptionImpl struct {
	bus     *EventBus
	subject string
	subIdx  int
}

func (s *SubscriptionImpl) Unsubscribe() {
	if s.bus == nil {
		return
	}

	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()

	if s.bus.closed {
		return
	}

	if subs, ok := s.bus.subjects[s.subject]; ok {
		delete(subs, s.subIdx)

		if len(subs) == 0 {
			delete(s.bus.subjects, s.subject)
		}
	}

	s.bus = nil
}

type EventBus struct {
	subjects             map[string]map[int]MessageHandler
	lastSubIdx           int
	mu                   sync.RWMutex
	closed               bool
	messageQueue         chan queuedMessage
	allMessagesProcessed chan struct{}
	done                 chan struct{}
	messageIsProcessing  bool
}

type queuedMessage struct {
	subject string
	message interface{}
}

func NewEventBus() *EventBus {
	eb := &EventBus{
		subjects:             make(map[string]map[int]MessageHandler),
		lastSubIdx:           0,
		closed:               false,
		messageQueue:         make(chan queuedMessage, queueSize),
		allMessagesProcessed: make(chan struct{}),
		messageIsProcessing:  false,
		done:                 make(chan struct{}),
	}

	eb.startQueueHandler()

	return eb
}

func (eb *EventBus) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	if cb == nil {
		return nil, errors.New("callback cannot be nil")
	}

	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.closed {
		return nil, ErrBusClosed
	}

	if _, ok := eb.subjects[subject]; !ok {
		eb.subjects[subject] = make(map[int]MessageHandler)
	}
	eb.lastSubIdx++
	eb.subjects[subject][eb.lastSubIdx] = cb
	return &SubscriptionImpl{eb, subject, eb.lastSubIdx}, nil
}

func (eb *EventBus) Publish(subject string, msg interface{}) error {
	eb.mu.RLock()
	if eb.closed {
		eb.mu.RUnlock()
		return ErrBusClosed
	}

	subs, hasSubscribers := eb.subjects[subject]

	if !hasSubscribers || len(subs) == 0 {
		eb.mu.RUnlock()
		return nil
	}
	eb.mu.RUnlock()

	eb.messageQueue <- queuedMessage{
		subject: subject,
		message: msg,
	}

	return nil
}

func (eb *EventBus) GetLenQueue() int {
	return len(eb.messageQueue)
}

func (eb *EventBus) startQueueHandler() {
	go func() {
		for {
			select {
			case <-eb.done:
				return
			case msg := <-eb.messageQueue:

				eb.mu.Lock()
				eb.messageIsProcessing = true
				eb.mu.Unlock()

				eb.mu.RLock()
				var handlers []MessageHandler
				if subs, ok := eb.subjects[msg.subject]; ok {
					handlers = make([]MessageHandler, 0, len(subs))
					for _, cb := range subs {
						handlers = append(handlers, cb)
					}
				}
				eb.mu.RUnlock()

				if len(handlers) > 0 {
					localWg := sync.WaitGroup{}
					localWg.Add(len(handlers))

					for _, handler := range handlers {
						go func(h MessageHandler) {
							defer func() {
								if r := recover(); r != nil {
									log.Printf("Recovered from panic in handler: %v", r)
								}
								localWg.Done()
							}()
							h(msg.message)
						}(handler)
					}

					localWg.Wait()
				}

				eb.mu.Lock()
				eb.messageIsProcessing = false
				if eb.closed && len(eb.messageQueue) == 0 {
					close(eb.allMessagesProcessed)
				}
				eb.mu.Unlock()

			}
		}
	}()
}

func (eb *EventBus) Close(ctx context.Context) error {
	eb.mu.Lock()

	if eb.closed {
		eb.mu.Unlock()
		return nil
	}

	eb.closed = true

	if len(eb.messageQueue) == 0 && !eb.messageIsProcessing {
		close(eb.allMessagesProcessed)
	}

	eb.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			eb.mu.Lock()
			close(eb.done)
			eb.subjects = make(map[string]map[int]MessageHandler)
			eb.lastSubIdx = 0
			eb.mu.Unlock()
			return ctx.Err()
		case <-eb.allMessagesProcessed:
			eb.mu.Lock()
			close(eb.done)
			eb.subjects = make(map[string]map[int]MessageHandler)
			eb.lastSubIdx = 0
			eb.mu.Unlock()
			return ctx.Err()
		}
	}
}
