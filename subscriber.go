package pubsub

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Subscriber struct {
	id       uint64
	messages chan *Message
	topics   map[string]bool
	active   bool
	lock     sync.RWMutex
}

func CreateNewSubscriber() (*Subscriber, error) {
	rand.Seed(time.Now().UnixNano())
	return &Subscriber{
		id:       rand.Uint64(),
		messages: make(chan *Message),
		topics:   map[string]bool{},
		active:   true,
	}, nil
}

func (s *Subscriber) AddTopic(topic string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	s.topics[topic] = true
	return nil
}

func (s *Subscriber) RemoveTopic(topic string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	delete(s.topics, topic)
	return nil
}

func (s *Subscriber) Close() error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	s.active = false
	close(s.messages)
	return nil
}

func (s *Subscriber) Send(msg *Message) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.active {
		s.messages <- msg
	}
	return nil
}

func (s *Subscriber) Listen() {
	for {
		msg, ok := <-s.messages
		if ok {
			fmt.Printf("Message %s received on topic %s by subscriber %d\n", msg.body, msg.topic, s.id)
		}
		//close loop if ends
		if !ok {
			break
		}
	}
}
