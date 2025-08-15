package main

import (
	"fmt"
	"log"
	"time"

	"github.com/radheem/ran-kafka-client-go/pkg/consumer"
	"github.com/radheem/ran-kafka-client-go/pkg/producer"
)

func main() {
	fmt.Println("=== Kafka Producer Example ===")
	if err := runProducerExample(); err != nil {
		log.Printf("Producer example failed: %v", err)
	}

	// Wait a moment before starting consumer
	time.Sleep(2 * time.Second)

	fmt.Println("\n=== Kafka Consumer Example ===")
	if err := runConsumerExample(); err != nil {
		log.Printf("Consumer example failed: %v", err)
	}
}

func runProducerExample() error {
	// Create producer configuration
	producerConfig := producer.Config{
		Brokers: []string{"localhost:9092"}, // Adjust to Kafka brokers
	}

	// Create new producer
	prod, err := producer.NewProducer(producerConfig)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer prod.Close()

	jsonMessage := producer.Message{
		Key:   "user-123",
		Value: map[string]interface{}{
			"user_id":   123,
			"action":    "login",
			"timestamp": time.Now().Unix(),
			"metadata": map[string]string{
				"source": "web",
				"ip":     "192.168.1.1",
			},
		},
		Headers: map[string]string{
			"content-type": "application/json",
			"source":       "user-service",
			"version":      "1.0",
		},
	}

	if err := prod.SendMessage("user-events", jsonMessage); err != nil {
		return fmt.Errorf("failed to send JSON message: %w", err)
	}

	// Example 2: Send a simple string message
	stringMessage := producer.Message{
		Key:   "notification-456",
		Value: "User 123 has logged in successfully",
		Headers: map[string]string{
			"content-type": "text/plain",
			"priority":     "normal",
		},
	}

	if err := prod.SendMessage("notifications", stringMessage); err != nil {
		return fmt.Errorf("failed to send string message: %w", err)
	}

	rawData := []byte(`{"order_id": 789, "status": "completed", "amount": 99.99}`)
	if err := prod.SendRawMessage("orders", "order-789", rawData, map[string]string{
		"content-type": "application/json",
		"service":      "order-service",
	}); err != nil {
		return fmt.Errorf("failed to send raw message: %w", err)
	}

	fmt.Println("Sending batch of messages...")
	for i := 0; i < 5; i++ {
		batchMessage := producer.Message{
			Key: fmt.Sprintf("batch-%d", i),
			Value: map[string]interface{}{
				"batch_id": i,
				"data":     fmt.Sprintf("This is message number %d", i),
				"created":  time.Now().Format(time.RFC3339),
			},
			Headers: map[string]string{
				"batch":      "true",
				"message_id": fmt.Sprintf("msg-%d", i),
			},
		}

		if err := prod.SendMessage("batch-messages", batchMessage); err != nil {
			log.Printf("Failed to send batch message %d: %v", i, err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("Producer examples completed successfully!")
	return nil
}

func runConsumerExample() error {
	// Create consumer configuration
	consumerConfig := consumer.Config{
		Brokers:       []string{"localhost:9092"}, // Kafka brokers
		Topics:        []string{"user-events", "notifications", "orders", "batch-messages"},
		ConsumerGroup: "example-consumer-group",
		// Optional MongoDB configuration - comment out if not using MongoDB
		MongoURI:        "mongodb://localhost:27017",
		MongoDB:         "kafka_messages",
		MongoCollection: "consumed_messages",
	}

	// Create new consumer
	kafkaConsumer, err := consumer.NewConsumer(consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	fmt.Println("Starting consumer...")
	fmt.Println("Press Ctrl+C to stop the consumer")
	
	// Start consuming messages
	// This will block until interrupted
	return kafkaConsumer.Start()
}
