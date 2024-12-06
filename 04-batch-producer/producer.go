package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"pulsar-test/tools"
	"time"
)

func main() {

	client := tools.CreateClientWithOauth2()
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   "xingcheng/xingcheng/test-topic",
		DisableBatching:         false,                  // 启用批量发送
		BatchingMaxPublishDelay: 100 * time.Millisecond, // 批量发送的最大延迟
		BatchingMaxMessages:     10,                     // 每批次的最大消息数
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		message := &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello %d", i)),
		}

		producer.SendAsync(ctx, message, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				fmt.Printf("Failed to publish message: %v\n", err)
			} else {
				fmt.Printf("Published message: %s\n", string(message.Payload))
			}
		})
	}

	// 等待所有异步操作完成
	producer.Flush()
}
