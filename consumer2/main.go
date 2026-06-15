package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

type Order struct {
	OrderID string `json:"order_id"`
	Item    string `json:"item"`
}

const consumerName = "Consumer-2"

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "orders",
		GroupID:        "order-processor", // SAME group as consumer 1 — important!
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Printf("\n[%s] Shutdown signal received, closing consumer...\n", consumerName)
		cancel()
	}()

	fmt.Printf("[%s] started — waiting for messages... (Ctrl+C to stop)\n", consumerName)
	fmt.Println("Group: order-processor | Topic: orders")
	fmt.Println("---")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Printf("[%s] stopped cleanly.\n", consumerName)
				break
			}
			log.Printf("[%s] error fetching message: %v", consumerName, err)
			break
		}

		var order Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("[%s] failed to unmarshal: %v", consumerName, err)
			continue
		}

		fmt.Printf("[%s] Received: %-10s | partition=%d offset=%d key=%s\n",
			consumerName, order.OrderID, msg.Partition, msg.Offset, string(msg.Key))

		if err := processOrder(order); err != nil {
			log.Printf("[%s] processing failed for %s, NOT committing: %v", consumerName, order.OrderID, err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[%s] failed to commit offset: %v", consumerName, err)
		}
	}
}

func processOrder(o Order) error {
	fmt.Printf("  -> [%s] processed %s (%s)\n", consumerName, o.OrderID, o.Item)
	return nil
}
