package main

// import (
// 	"fmt"
// 	"log"
// 	"time"

// 	"github.com/IBM/sarama"
// )

// func main() {
// 	config := sarama.NewConfig()
// 	config.Producer.RequiredAcks = sarama.WaitForAll
// 	config.Producer.Return.Successes = true
// 	config.Consumer.Return.Errors = true

// 	brokers := []string{"localhost:9093"}

// 	// Test connection with timeout
// 	fmt.Println("Testing Kafka connection...")

// 	client, err := sarama.NewClient(brokers, config)
// 	if err != nil {
// 		log.Printf("Failed to create client: %v", err)

// 		// Try with more retries and longer timeout
// 		fmt.Println("Retrying with extended configuration...")
// 		config.Net.DialTimeout = 30 * time.Second
// 		config.Net.ReadTimeout = 30 * time.Second
// 		config.Net.WriteTimeout = 30 * time.Second
// 		config.Metadata.Retry.Max = 10
// 		config.Metadata.Retry.Backoff = 2 * time.Second
// 		config.Metadata.RefreshFrequency = 10 * time.Minute

// 		client, err = sarama.NewClient(brokers, config)
// 		if err != nil {
// 			log.Fatalf("Still failed to create client: %v", err)
// 		}
// 	}
// 	defer client.Close()

// 	fmt.Println("Client created successfully!")

// 	// Test getting metadata
// 	topics, err := client.Topics()
// 	if err != nil {
// 		log.Printf("Failed to get topics: %v", err)
// 	} else {
// 		fmt.Printf("Available topics: %v\n", topics)
// 	}

// 	// Test creating a producer
// 	producer, err := sarama.NewSyncProducerFromClient(client)
// 	if err != nil {
// 		log.Printf("Failed to create producer: %v", err)
// 	} else {
// 		fmt.Println("Producer created successfully!")
// 		producer.Close()
// 	}

// 	fmt.Println("Connection test completed successfully!")
// }
