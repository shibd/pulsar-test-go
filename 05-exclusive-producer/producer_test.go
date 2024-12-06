package main

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"pulsar-test/tools"
	"strings"
	"testing"
)

func TestExclusiveProducer(t *testing.T) {

	client := tools.CreateClientWithOauth2()
	defer client.Close()

	defer client.Close()

	topicName := "xingcheng/xingcheng/test-topic"
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:              topicName,
		ProducerAccessMode: pulsar.ProducerAccessModeExclusive,
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	_, err = client.CreateProducer(pulsar.ProducerOptions{
		Topic:              topicName,
		ProducerAccessMode: pulsar.ProducerAccessModeExclusive,
	})
	assert.Error(t, err, "Producer should be fenced")
	assert.True(t, strings.Contains(err.Error(), "ProducerFenced"))

	_, err = client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})
	assert.Error(t, err, "Producer should be failed")
	assert.True(t, strings.Contains(err.Error(), "ProducerBusy"))

}
