package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Writer = Producer in segmentio/kafka-go
	writer := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "orders",
		Balancer:     &kafka.Hash{}, // hash(key) % partitions
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	defer writer.Close()

	orders := []struct {
		RestaurantID string
		OrderID      string
		Item         string
	}{
		{"restaurant_42", "order#1", "Biryani"},
		{"restaurant_99", "order#2", "Pizza"},
		{"restaurant_42", "order#3", "Butter Naan"},
		{"restaurant_99", "order#4", "Pasta"},
		{"restaurant_42", "order#5", "Lassi"},
		{"restaurant_99", "order#6", "Garlic Bread"},
	}

	ctx := context.Background()

	for _, o := range orders {
		msg := kafka.Message{
			Key:   []byte(o.RestaurantID),
			Value: []byte(fmt.Sprintf(`{"order_id":"%s","item":"%s"}`, o.OrderID, o.Item)),
			Time:  time.Now(),
		}

		err := writer.WriteMessages(ctx, msg)
		if err != nil {
			log.Printf("failed to write message %s: %v", o.OrderID, err)
			continue
		}

		fmt.Printf("Produced %s (key=%s) -> %s\n", o.OrderID, o.RestaurantID, o.Item)
	}

	fmt.Println("\nAll messages produced!")
}
