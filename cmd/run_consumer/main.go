package run_consumer

import (
	"log"

	consumer "github.com/radheem/ran-kafka-client-go/pkg/consumer"
)

func ExecuteConsumer(port string, topics []string, consumerGroup string, mongoURI string, mongoDB string, mongoCollection string ) {
    if (port == ""){
        return 
    }
    brokers := []string{"localhost:"+port}
    
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