package run_producer

import (
	"log"

	"fmt"
	"time"

	"github.com/joho/godotenv"
	producer "github.com/radheem/ran-kafka-client-go/pkg/producer"
)

// import env params
func init() {
	// Load environment variables using a package like "github.com/joho/godotenv"
	godotenv.Load(".env")
}

func ExecuteProducer(kafkaPort string, kafkaTopic string, count int) {
	if (kafkaPort == "") {
		kafkaPort = "9092"
	}
	if (kafkaTopic == ""){
		kafkaTopic = "my-topic"
	}
	if(count < 0){
		return
	}
	config := producer.Config{
		Brokers: []string{"localhost:" + kafkaPort}, // Kafka broker addresses
	}

	// Create the producer
	prod, err := producer.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer prod.Close()
	stringMsg := true;
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

	stringMessage := producer.Message{
		Key:   "notification-456",
		Value: "Hello, Kafka!",
		Headers: map[string]string{
			"content-type": "text/plain",
		},
	}
	itr_count := 0 
	for (itr_count < count) {
		if (stringMsg) {
			if err := prod.SendMessage(kafkaTopic, stringMessage); err != nil {
				log.Printf("Failed to send string message: %v", err)
			} else {
				fmt.Println("String message sent successfully!")
			}
		}else{
			if err := prod.SendMessage(kafkaTopic, message); err != nil {
				log.Printf("Failed to send message: %v", err)
			} else {
				fmt.Println("Message sent successfully!")
			}
		}
		itr_count++
	}
	

	
}
