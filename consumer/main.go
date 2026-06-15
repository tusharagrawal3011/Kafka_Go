package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type Order struct {
	OrderID string `json:"order_id"`
	Item    string `json:"item"`
}

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "orders",
		GroupID:        "order-processor",
		CommitInterval: 0, // manual commit
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	ctx := context.Background()

	fmt.Println("Consumer started — waiting for messages...")
	fmt.Println("Group: order-processor | Topic: orders")
	fmt.Println("---")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("error fetching message: %v", err)
			break
		}

		var order Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("failed to unmarshal: %v", err)
			continue
		}

		fmt.Printf("Received: %-10s | partition=%d offset=%d key=%s\n",
			order.OrderID, msg.Partition, msg.Offset, string(msg.Key))

		if err := processOrder(order); err != nil {
			log.Printf("processing failed for %s, NOT committing: %v", order.OrderID, err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("failed to commit offset: %v", err)
		}
	}
}

func processOrder(o Order) error {
	fmt.Printf("  -> processed %s (%s)\n", o.OrderID, o.Item)
	return nil
}
