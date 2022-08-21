package main

import (
	"fmt"
	"time"

	"github.com/urishabh12/pubsub"
)

func AddToTopic(b *pubsub.Broker) {
	topics := map[string]string{
		"Cat": "Catoma",
		"Dog": "Dogoma",
		"Pig": "Pigoma",
		"Lab": "Laboma",
	}

	for {
		for k, v := range topics {
			b.Publish(k, v)
		}
		fmt.Println("<=========================================>")
		time.Sleep(2 * time.Second)
	}
}

func main() {
	b, _ := pubsub.NewBroker()
	s1, _ := b.AddSubscriber()
	s2, _ := b.AddSubscriber()

	b.Subscribe(s1, "Cat")
	b.Subscribe(s1, "Dog")
	b.Subscribe(s2, "Dog")

	go (func() {
		time.Sleep(6 * time.Second)
		b.Subscribe(s2, "Lab")
	})()

	go (func() {
		time.Sleep(8 * time.Second)
		b.Unsubscribe(s1, "Dog")
	})()

	go (func() {
		time.Sleep(12 * time.Second)
		b.RemoveSubscriber(s1)
	})()

	go s1.Listen()
	go s2.Listen()

	AddToTopic(b)

	fmt.Scanln()
	fmt.Println("Service Closed")
}
