package pubsub

import (
	"errors"
	"sync"
)

type Subscribers map[uint64]*Subscriber

type Broker struct {
	subscribers Subscribers
	topics      map[string]Subscribers
	lock        sync.RWMutex
}

func NewBroker() (*Broker, error) {
	return &Broker{
		subscribers: Subscribers{},
		topics:      map[string]Subscribers{},
	}, nil
}

func (b *Broker) AddSubscriber() (*Subscriber, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	sub, err := CreateNewSubscriber()
	if err != nil {
		return nil, errors.New("error creating a new subscriber")
	}
	b.subscribers[sub.id] = sub
	return sub, nil
}

func (b *Broker) RemoveSubscriber(s *Subscriber) error {
	for topic := range s.topics {
		b.Unsubscribe(s, topic)
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	delete(b.subscribers, s.id)
	s.Close()
	return nil
}

func (b *Broker) Subscribe(s *Subscriber, topic string) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.topics[topic] == nil {
		b.topics[topic] = Subscribers{}
	}
	s.AddTopic(topic)
	b.topics[topic][s.id] = s
	return nil
}

func (b *Broker) Unsubscribe(s *Subscriber, topic string) error {
	b.lock.RLock()
	defer b.lock.RUnlock()
	delete(b.topics[topic], s.id)
	s.RemoveTopic(topic)
	return nil
}

func (b *Broker) Publish(topic string, msg string) error {
	b.lock.Lock()
	subscribers := b.topics[topic]
	b.lock.Unlock()
	m := NewMessage(msg, topic)
	for _, s := range subscribers {
		if !s.active {
			continue
		}

		go (func(s *Subscriber) {
			s.Send(m)
		})(s)
	}
	return nil
}
