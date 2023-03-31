package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "my-topic",
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})

	defer consumer.Close()

	msg, err := consumer.Receive(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
		msg.ID(), string(msg.Payload()))

}
