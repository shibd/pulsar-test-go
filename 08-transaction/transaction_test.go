package main

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pulsar-test/tools"
	"testing"
	"time"
)

// TestConsumeAndProduceWithTxn is a test function that validates the behavior of producing and consuming
// messages with and without transactions. It consists of the following steps:
//
// 1. Prepare: Create a PulsarClient and initialize the transaction coordinator client.
// 2. Prepare: Create a topic and a subscription.
// 3. Produce 10 messages with a transaction and 10 messages without a transaction.
// - Expectation: The consumer should be able to receive the 10 messages sent without a transaction,
// but not the 10 messages sent with the transaction.
// 4. Commit the transaction and receive the remaining 10 messages.
// - Expectation: The consumer should be able to receive the 10 messages sent with the transaction.
// 5. Clean up: Close the consumer and producer instances.
//
// The test ensures that the consumer can only receive messages sent with a transaction after it is committed,
// and that it can always receive messages sent without a transaction.
func TestConsumeAndProduceWithTxn(t *testing.T) {
	// Step 1: Prepare - Create PulsarClient and initialize the transaction coordinator client.
	topic := "xingcheng/xingcheng/transaction-topic"
	sub := "my-sub"

	client := tools.CreateClientWithOauth2WithTransaction()
	defer client.Close()

	// Step 2: Prepare - Create Topic and Subscription.
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
	})
	assert.NoError(t, err)
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:       topic,
		SendTimeout: 0,
	})
	assert.NoError(t, err)
	// Step 3: Open a transaction, send 10 messages with the transaction and 10 messages without the transaction.
	// Expectation: We can receive the 10 messages sent without a transaction and
	// cannot receive the 10 messages sent with the transaction.
	txn, err := client.NewTransaction(time.Hour)
	require.Nil(t, err)
	for i := 0; i < 10; i++ {
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: make([]byte, 1024),
		})
		require.Nil(t, err)
	}
	for i := 0; i < 10; i++ {
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Transaction: txn,
			Payload:     make([]byte, 1024),
		})
		require.Nil(t, err)
	}
	// Attempt to receive and acknowledge the 10 messages sent without a transaction.
	for i := 0; i < 10; i++ {
		msg, _ := consumer.Receive(context.Background())
		assert.NotNil(t, msg)
		err = consumer.Ack(msg)
		assert.Nil(t, err)
	}
	// Create a goroutine to attempt receiving a message and send it to the 'done1' channel.
	done1 := make(chan pulsar.Message)
	go func() {
		msg, _ := consumer.Receive(context.Background())
		err := consumer.AckID(msg.ID())
		require.Nil(t, err)
		close(done1)
	}()
	// Expectation: The consumer should not receive uncommitted messages.
	select {
	case <-done1:
		require.Fail(t, "The consumer should not receive uncommitted message")
	case <-time.After(time.Second):
	}
	// Step 4: After committing the transaction, we should be able to receive the remaining 10 messages.
	// Acknowledge the rest of the 10 messages with the transaction.
	// Expectation: After committing the transaction, all messages of the subscription will be acknowledged.
	_ = txn.Commit(context.Background())
	txn, err = client.NewTransaction(time.Hour)
	require.Nil(t, err)
	for i := 0; i < 9; i++ {
		msg, _ := consumer.Receive(context.Background())
		require.NotNil(t, msg)
		err = consumer.AckWithTxn(msg, txn)
		require.Nil(t, err)
	}
	_ = txn.Commit(context.Background())
	<-done1
	consumer.Close()
	consumer, _ = client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
	})
	// Create a goroutine to attempt receiving a message and send it to the 'done1' channel.
	done2 := make(chan pulsar.Message)
	go func() {
		consumer.Receive(context.Background())
		close(done2)
	}()

	// Expectation: The consumer should not receive uncommitted messages.
	select {
	case <-done2:
		require.Fail(t, "The consumer should not receive messages")
	case <-time.After(time.Second):
	}

	// Step 5: Clean up - Close the consumer and producer instances.
	consumer.Close()
	producer.Close()
}
