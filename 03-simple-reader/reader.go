package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"pulsar-test/tools"
)

func main() {

	client := tools.CreateClientWithOauth2()
	defer client.Close()

	defer client.Close()

	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          "xingcheng/xingcheng/test-topic",
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Reader message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))
	}

}
