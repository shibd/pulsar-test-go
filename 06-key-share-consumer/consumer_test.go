package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"pulsar-test/tools"
	"testing"
)

func TestConsumerKeyShared(t *testing.T) {
	client := tools.CreateClientWithOauth2()
	defer client.Close()

	topic := "xingcheng/xingcheng/key-share-topic2"

	consumer1, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             pulsar.KeyShared,
		Name:             "consumer-1",
	})
	assert.Nil(t, err)
	defer consumer1.Close()

	consumer2, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             pulsar.KeyShared,
		Name:             "consumer-2",
	})
	assert.Nil(t, err)
	defer consumer2.Close()

	// Create producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Key:     fmt.Sprintf("key-shared-%d", i%3),
			Payload: []byte(fmt.Sprintf("value-%d", i)),
		})
		assert.Nil(t, err)
	}

	receivedConsumer1 := 0
	receivedConsumer2 := 0
	for (receivedConsumer1 + receivedConsumer2) < 100 {
		select {
		case cm, ok := <-consumer1.Chan():
			if !ok {
				break
			}
			receivedConsumer1++
			fmt.Printf("Consumer 1 received message: Key=%s, Value=%s\n", cm.Key(), string(cm.Payload()))
			consumer1.Ack(cm.Message)
		case cm, ok := <-consumer2.Chan():
			if !ok {
				break
			}
			receivedConsumer2++
			fmt.Printf("Consumer 2 received message: Key=%s, Value=%s\n", cm.Key(), string(cm.Payload()))
			consumer2.Ack(cm.Message)
		}
	}

	assert.NotEqual(t, 0, receivedConsumer1)
	assert.NotEqual(t, 0, receivedConsumer2)

	t.Logf("TestConsumerKeyShared received messages consumer1: %d consumer2: %d\n",
		receivedConsumer1, receivedConsumer2)
	assert.Equal(t, 100, receivedConsumer1+receivedConsumer2)
}
