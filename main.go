package main

import (
	"log"

	"fmt"
	"time"

	"github.com/joho/godotenv"
	consumer "github.com/radheem/ran-kafka-client-go/pkg/consumer"
	producer "github.com/radheem/ran-kafka-client-go/pkg/producer"
)

// import env params
func init() {
	// Load environment variables using a package like "github.com/joho/godotenv"
	godotenv.Load(".env")
}

func executeProducer(kafkaPort string, kafkaTopic string) {
	config := producer.Config{
		Brokers: []string{"localhost:" + kafkaPort}, // Your Kafka broker addresses
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
		Value: map[string]any{
			"user_id":   123,
			"action":    "login",
			"timestamp": time.Now().Unix(),
		},
		Headers: map[string]string{
			"content-type": "application/json",
			"source":       "user-service",
		},
	}

	if err := prod.SendMessage(kafkaTopic, message); err != nil {
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

	if err := prod.SendMessage(kafkaTopic, stringMessage); err != nil {
		log.Printf("Failed to send string message: %v", err)
	} else {
		fmt.Println("String message sent successfully!")
	}
}

func executeConsumer(kafkaPort string, mongoPort string, kafkaTopic string) {
	config := consumer.Config{
		Brokers:       []string{"localhost:" + kafkaPort}, // Your Kafka broker addresses
		Topics:        []string{kafkaTopic},       // Topics to consume from
		ConsumerGroup: "test",        // Consumer group ID

		MongoURI:        "mongodb://root:password@localhost:" + mongoPort,
		MongoDB:         "kafka_db",
		MongoCollection: "messages",
	}

	// Create the consumer
	kafkaConsumer, err := consumer.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	log.Println("Starting Kafka consumer...")
	log.Println("Press Ctrl+C to stop")

	// Start consuming messages (this blocks until interrupted)
	if err := kafkaConsumer.Start(); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}

}
func main() {
	kafkaPort := "9092" // Default Kafka port
	kafkaTopic := "my-topic" // Default topic
	mongoPort := "27017" // Default MongoDB port
	// Example 1: Basic Producer Usage
	fmt.Println("=== Kafka Producer Example ===")
	executeProducer(kafkaPort, kafkaTopic)

	// Wait a moment before starting consumer
	time.Sleep(2 * time.Second)

	// Example 2: Basic Consumer Usage
	fmt.Println("\n=== Kafka Consumer Example ===")
	executeConsumer(kafkaPort, mongoPort,kafkaTopic)
}