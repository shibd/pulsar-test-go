package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"pulsar-test/tools"
	"time"
)

func main() {

	client := tools.CreateClientWithOauth2()
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "xingcheng/xingcheng/delay-topic",
	})
	defer producer.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "xingcheng/xingcheng/delay-topic",
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})
	defer consumer.Close()

	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			// delay 5 minutes
			_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
				Payload:   []byte(fmt.Sprintf("hello delay %d", i)),
				DeliverAt: time.Now().Add(5 * time.Minute),
			})
		} else {
			_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
				Payload: []byte(fmt.Sprintf("hello %d", i)),
			})
		}

		if err != nil {
			fmt.Printf("Failed to publish message %d: %v\n", i, err)
		} else {
			fmt.Printf("Published message %d\n", i)
		}
	}

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		// Acknowledge the message
		_ = consumer.Ack(msg)
	}

}
