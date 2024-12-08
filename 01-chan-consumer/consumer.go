package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"os"
	"pulsar-test/tools"
	"time"
)

func main() {

	client := tools.CreateClientWithOauth2()
	defer client.Close()

	// k8s pod name is used as the consumer name
	// env:
	//	- name: POD_NAME
	//    valueFrom:
	//      fieldRef:
	//      fieldPath: metadata.name
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "xingcheng/xingcheng/test-topic",
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
		Name:             os.Getenv("POD_NAME"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		select {
		case cm, ok := <-consumer.Chan():
			if !ok {
				return
			}
			fmt.Printf("Consumer received message: Key=%s, Value=%s\n", cm.Key(), string(cm.Payload()))
			consumer.Ack(cm.Message)
		case <-ctx.Done():
			fmt.Println("Timeout occurred, no message received in the last 5 seconds")
		}
	}
}
