package main

import (
	"fmt"
	"log"
	"time"

	"github.com/radheem/ran-kafka-client-go/pkg/producer"
)

func producerMain() {
	// Configure the producer
	config := producer.Config{
		Brokers: []string{"localhost:9092"}, // Kafka broker addresses
	}

	// Create the producer
	prod, err := producer.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer prod.Close()

	// Example 1: Send a JSON message
	message := producer.Message{
		Key:   "user-123",
		Value: map[string]interface{}{
			"user_id":   123,
			"action":    "login",
			"timestamp": time.Now().Unix(),
		},
		Headers: map[string]string{
			"content-type": "application/json",
			"source":       "user-service",
		},
	}

	if err := prod.SendMessage("my-topic", message); err != nil {
		log.Printf("Failed to send message: %v", err)
	} else {
		fmt.Println("Message sent successfully!")
	}

	// Example 2: Send a simple string message
	stringMessage := producer.Message{
		Key:   "notification-456",
		Value: "Hello, Kafka!",
		Headers: map[string]string{
			"content-type": "text/plain",
		},
	}

	if err := prod.SendMessage("my-topic", stringMessage); err != nil {
		log.Printf("Failed to send string message: %v", err)
	} else {
		fmt.Println("String message sent successfully!")
	}
}
