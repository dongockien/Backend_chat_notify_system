package kafka

import (
	"chat-notify-system/internal/models"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

var Writer *kafka.Writer

func InitProducer(brokers []string, topic string) {
	Writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafka.RequireAll, 
		Async:        false,            
	}
}

func PublishEvent(ctx context.Context, event models.MessageSentEvent) error {
	if Writer == nil {
		return fmt.Errorf("kafka writer chưa được khởi tạo")
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event failed: %w", err)
	}
	return Writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", event.ConversationID)), 
		Value: payload,
	})
}

func PublishToDLQ(ctx context.Context, key []byte, value []byte) error {
	if Writer == nil {
		return fmt.Errorf("kafka writer chưa được khởi tạo")
	}
	return Writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}