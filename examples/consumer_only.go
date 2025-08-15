package main

import (
	"log"

	"github.com/radheem/ran-kafka-client-go/pkg/consumer"
)

func consumerMain() {
	// Configure the consumer
	config := consumer.Config{
		Brokers:       []string{"localhost:9092"}, // Kafka broker addresses
		Topics:        []string{"my-topic"},       // Topics to consume from
		ConsumerGroup: "my-consumer-group",        // Consumer group ID
		
		// Optional: MongoDB storage configuration
		// Comment out these lines if you don't want to store messages in MongoDB
		MongoURI:        "mongodb://localhost:27017",
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
