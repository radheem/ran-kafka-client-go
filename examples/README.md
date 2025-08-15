# Kafka Go Client

This directory contains example usage scripts for the Kafka Go client library.

## Examples

### 1. Complete Usage Example (`usage_example.go`)
Demonstrates both producer and consumer functionality with various message types and configurations.

### 2. Consumer Only Example (`consumer_only.go`)
Simple example showing how to set up and run a Kafka consumer.

### 3. Producer Only Example (`producer_only.go`)
Simple example showing how to send messages to Kafka topics.

## Prerequisites

Before running these examples, make sure you have:

1. **Kafka running locally** on `localhost:9092` (or update the broker addresses in the examples)
2. **MongoDB running locally** on `localhost:27017` (optional, for message storage)
3. **Go modules initialized** in project

## Running the Examples

### Setup
```bash
# Make sure you're in the project root
cd /home/radr/projects/kafka-go-client

# Install dependencies
go mod tidy
```

### Run Individual Examples

```bash
# Run the complete usage example
go run examples/usage_example.go

# Run only the consumer example
go run examples/consumer_only.go

# Run only the producer example
go run examples/producer_only.go
```

## Configuration Options

### Consumer Configuration
```go
config := consumer.Config{
    Brokers:         []string{"localhost:9092"},    // Kafka broker addresses
    Topics:          []string{"my-topic"},          // Topics to consume from
    ConsumerGroup:   "my-consumer-group",           // Consumer group ID
    
    // Optional MongoDB storage
    MongoURI:        "mongodb://localhost:27017",   // MongoDB connection string
    MongoDB:         "kafka_db",                    // Database name
    MongoCollection: "messages",                    // Collection name
}
```

### Producer Configuration
```go
config := producer.Config{
    Brokers: []string{"localhost:9092"},    // Kafka broker addresses
}
```

## Message Types

The library supports various message types:

### JSON Messages
```go
message := producer.Message{
    Key:   "user-123",
    Value: map[string]interface{}{
        "user_id": 123,
        "action":  "login",
    },
    Headers: map[string]string{
        "content-type": "application/json",
    },
}
```

### String Messages
```go
message := producer.Message{
    Key:   "notification-456",
    Value: "Hello, Kafka!",
    Headers: map[string]string{
        "content-type": "text/plain",
    },
}
```

### Raw Byte Messages
```go
rawData := []byte(`{"order_id": 789}`)
err := producer.SendRawMessage("orders", "order-789", rawData, headers)
```

## Consumer Features

- **Automatic JSON parsing**: Messages are automatically parsed as JSON when possible
- **MongoDB storage**: Optionally store consumed messages in MongoDB
- **Graceful shutdown**: Handles SIGINT and SIGTERM signals
- **Consumer group management**: Automatic rebalancing and offset management
- **Header support**: Full support for Kafka message headers

## Producer Features

- **JSON serialization**: Automatic JSON marshaling for structured data
- **Header support**: Full support for custom message headers
- **Delivery guarantees**: Configured for at-least-once delivery
- **Compression**: Uses Snappy compression for better performance
- **Batch processing**: Efficient batching with configurable flush intervals

## Error Handling

Both producer and consumer include comprehensive error handling:

- Connection failures are logged and retried
- Message processing errors are logged but don't stop the consumer
- MongoDB connection issues are handled gracefully

## Monitoring

The library includes built-in logging for:

- Message production and consumption events
- MongoDB storage operations
- Connection status updates
- Error conditions

## Troubleshooting

### Common Issues

1. **Connection refused**: Make sure Kafka is running on the specified brokers
2. **MongoDB errors**: Ensure MongoDB is running if you're using message storage
3. **Topic doesn't exist**: Create the topic manually or enable auto-topic creation in Kafka
4. **Consumer group conflicts**: Use unique consumer group names for different applications

### Logs

Check the console output for detailed logs about:
- Connection status
- Message processing
- Error conditions
- MongoDB operations
