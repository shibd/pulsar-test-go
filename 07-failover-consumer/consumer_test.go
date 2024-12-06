package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"pulsar-test/tools"
	"sync"
	"testing"
)

func TestConsumerFailover(t *testing.T) {
	client := tools.CreateClientWithOauth2()
	defer client.Close()

	topic := "xingcheng/xingcheng/failover-topic"

	consumer1, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "sub-failover",
		Type:             pulsar.Failover,
		Name:             "consumer-1",
	})
	assert.Nil(t, err)
	defer consumer1.Close()

	consumer2, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "sub-failover",
		Type:             pulsar.Failover,
		Name:             "consumer-2",
	})
	assert.Nil(t, err)
	defer consumer2.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("value-%d", i)),
		})
		assert.Nil(t, err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	receivedConsumer1 := 0
	receivedConsumer2 := 0
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	consume := func(consumer pulsar.Consumer, receivedCount *int, done chan struct{}) {
		defer wg.Done()
		for {
			select {
			case cm, ok := <-consumer.Chan():
				if !ok {
					return
				}
				(*receivedCount)++
				fmt.Printf("Consumer %s received message: %s\n", consumer.Name(), string(cm.Payload()))
				consumer.Ack(cm.Message)
				if *receivedCount >= 5 {
					consumer.Close()
					close(done)
					return
				}
			case <-done:
				return
			}
		}
	}

	go consume(consumer1, &receivedConsumer1, done1)
	go consume(consumer2, &receivedConsumer2, done2)

	wg.Wait()

	totalReceived := receivedConsumer1 + receivedConsumer2
	assert.Equal(t, 10, totalReceived)

	t.Logf("TestConsumerFailover received messages consumer1: %d consumer2: %d\n",
		receivedConsumer1, receivedConsumer2)
}
