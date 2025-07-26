package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "new-token-events",
		GroupID:   "db-manage-group",
		Partition: 0,
		MinBytes:  1, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	defer reader.Close()

	fmt.Println("Kafka Consumer 시작...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("failed to read message: %v", err)
		}
		now := time.Now()
		fmt.Printf("%s, Received: key=%s value=%s\n", now.Format("2006-01-02 15:04:05.000"), string(msg.Key), string(msg.Value))
	}
}
