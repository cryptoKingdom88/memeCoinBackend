package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/collectorService/config"

	"github.com/segmentio/kafka-go"
)

var writer *kafka.Writer

func SendKafkaMessage(topic string, key, value []byte) error {
	if writer == nil {
		log.Println("Need to initialize Kafka writer")
		return fmt.Errorf("kafka writer is not initialized")
	}
	msg := kafka.Message{
		Topic: topic,
		Value: value,
	}

	err := writer.WriteMessages(context.Background(), msg)
	if err != nil {
		log.Printf("Kafka send error: %v", err)
	}
	return err
}

func Produce(msg []byte) {
	if writer == nil {
		log.Println("Need to initialize Kafka writer")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := writer.WriteMessages(ctx, kafka.Message{
		Value: msg,
	})
	if err != nil {
		log.Printf("Kafka Transmit Failed: %v", err)
	} else {
		log.Println("Kafka Transmit Success")
	}
}

func Init(cfg config.Config) {
	writer = &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers),
		Balancer: &kafka.LeastBytes{},
		Async:    true,
	}
}
