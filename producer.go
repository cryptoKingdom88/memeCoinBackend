package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "new-token-events",
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1,                     // No batch send
		BatchTimeout: 10 * time.Millisecond, // Minize send delay
	})

	defer writer.Close()

	for i := 0; i < 100000; i++ {
		msg := fmt.Sprintf("Message #%d", i)
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(fmt.Sprintf("Key-%d", i)),
				Value: []byte(msg),
			},
		)
		if err != nil {
			log.Fatalf("failed to write messages: %v", err)
		}
		now := time.Now()

		fmt.Printf("%s, Produced: %s\n", now.Format("2006-01-02 15:04:05.000"), msg)
		time.Sleep(50 * time.Millisecond)
	}
}
