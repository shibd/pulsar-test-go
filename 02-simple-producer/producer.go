package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"pulsar-test/tools"
)

func main() {

	client := tools.CreateClientWithOauth2()
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "xingcheng/xingcheng/test-topic",
	})
	defer producer.Close()

	for i := 0; i < 10; i++ {
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello %d", i)),
		})

		if err != nil {
			fmt.Printf("Failed to publish message %d: %v\n", i, err)
		} else {
			fmt.Printf("Published message %d\n", i)
		}
	}

}
