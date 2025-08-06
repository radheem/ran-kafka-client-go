package main

import (
	"log"
	"oran-kafka-go-client/pkg/consumer"
	"os"
	"strings"
)

func main() {
    brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
    topics := strings.Split(os.Getenv("KAFKA_TOPICS"), ",")
    consumerGroup := os.Getenv("CONSUMER_GROUP")
    mongoURI := os.Getenv("MONGO_URI")
    mongoDB := os.Getenv("MONGO_DB")
    mongoCollection := os.Getenv("MONGO_COLLECTION")

    if len(brokers) == 0 || brokers[0] == "" {
        log.Fatal("KAFKA_BROKERS environment variable is required")
    }
    if len(topics) == 0 || topics[0] == "" {
        log.Fatal("KAFKA_TOPICS environment variable is required")
    }
    if consumerGroup == "" {
        consumerGroup = "default-consumer-group"
    }
    if mongoDB == "" {
        mongoDB = "kafka_data"
    }
    if mongoCollection == "" {
        mongoCollection = "messages"
    }

    config := consumer.Config{
        Brokers:         brokers,
        Topics:          topics,
        ConsumerGroup:   consumerGroup,
        MongoURI:        mongoURI,
        MongoDB:         mongoDB,
        MongoCollection: mongoCollection,
    }

    c, err := consumer.NewConsumer(config)
    if err != nil {
        log.Fatal("Failed to create consumer:", err)
    }

    log.Printf("Starting consumer with config: %+v", config)
    if err := c.Start(); err != nil {
        log.Fatal("Failed to start consumer:", err)
    }
}