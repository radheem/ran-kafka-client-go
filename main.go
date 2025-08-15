package main

import (
	"flag"
	"fmt"

	"github.com/joho/godotenv"
	consumer "github.com/radheem/ran-kafka-client-go/cmd/run_consumer"
	producer "github.com/radheem/ran-kafka-client-go/cmd/run_producer"
)

// import env params
func init() {
	godotenv.Load(".env")
}

func main() {
	mode := flag.String("mode", "consumer","specify which you want to run producer/consumer, default consumer")
	port := flag.String("port", "9092", "the port kafka is exposed on")
	topic := flag.String("kafkaTopic","my-topic", "default is my-topic")
	msgCount := flag.Int("msgcount", 20, "the number of messages you want published")
	mongoURI := "mongodb://localhost:27017" 
	// Parse the command-line flags
	flag.Parse()

	// Print all defined flags and their values
	fmt.Println("All defined flags and their values:")
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("  %s: %v (default: %v)\n", f.Name, f.Value, f.DefValue)
	})
	topics := []string{*topic}
	
	if (*mode == "producer"){
		producer.ExecuteProducer(*port, *topic, *msgCount)
	}else{
		consumer.ExecuteConsumer(*port, topics, "example-consumer-group", mongoURI,"kafka-messages","consumed_messages")
	}
}