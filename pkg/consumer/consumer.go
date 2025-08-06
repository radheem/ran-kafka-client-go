package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
    Brokers       []string
    Topics        []string
    ConsumerGroup string
    MongoURI      string
    MongoDB       string
    MongoCollection string
}

type Consumer struct {
    config       Config
    client       sarama.ConsumerGroup
    mongoClient  *mongo.Client
    mongoCollection *mongo.Collection
    ready        chan bool
    ctx          context.Context
    cancel       context.CancelFunc
    wg           sync.WaitGroup
}

type Message struct {
    Topic     string                 `json:"topic"`
    Partition int32                  `json:"partition"`
    Offset    int64                  `json:"offset"`
    Key       string                 `json:"key,omitempty"`
    Value     any                    `json:"value"`
    Headers   map[string]string      `json:"headers,omitempty"`
    Timestamp time.Time              `json:"timestamp"`
}

func NewConsumer(config Config) (*Consumer, error) {
    // Setup Sarama configuration
    saramaConfig := sarama.NewConfig()
    saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
    saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
    saramaConfig.Consumer.Group.Session.Timeout = 10 * time.Second
    saramaConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second

    // Create consumer group client
    client, err := sarama.NewConsumerGroup(config.Brokers, config.ConsumerGroup, saramaConfig)
    if err != nil {
        return nil, fmt.Errorf("failed to create consumer group: %w", err)
    }

    ctx, cancel := context.WithCancel(context.Background())

    consumer := &Consumer{
        config: config,
        client: client,
        ready:  make(chan bool),
        ctx:    ctx,
        cancel: cancel,
    }

    // Setup MongoDB if configured
    if config.MongoURI != "" {
        if err := consumer.setupMongo(); err != nil {
            return nil, fmt.Errorf("failed to setup MongoDB: %w", err)
        }
    }

    return consumer, nil
}

func (c *Consumer) setupMongo() error {
    clientOptions := options.Client().ApplyURI(c.config.MongoURI)
    client, err := mongo.Connect(c.ctx, clientOptions)
    if err != nil {
        return err
    }

    // Test connection
    if err := client.Ping(c.ctx, nil); err != nil {
        return err
    }

    c.mongoClient = client
    c.mongoCollection = client.Database(c.config.MongoDB).Collection(c.config.MongoCollection)
    
    log.Printf("Connected to MongoDB: %s/%s", c.config.MongoDB, c.config.MongoCollection)
    return nil
}

func (c *Consumer) Start() error {
    log.Printf("Starting consumer for topics: %v", c.config.Topics)

    c.wg.Add(1)
    go func() {
        defer c.wg.Done()
        for {
            select {
            case <-c.ctx.Done():
                log.Println("Consumer context cancelled")
                return
            default:
                if err := c.client.Consume(c.ctx, c.config.Topics, c); err != nil {
                    log.Printf("Error from consumer: %v", err)
                    return
                }
            }
        }
    }()

    // Wait for consumer to be ready
    <-c.ready
    log.Println("Consumer is ready and consuming messages")
    
    // Setup signal handling
    sigterm := make(chan os.Signal, 1)
    signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

    select {
    case <-sigterm:
        log.Println("Termination signal received")
    case <-c.ctx.Done():
        log.Println("Consumer context done")
    }

    c.Stop()
    return nil
}

func (c *Consumer) Stop() {
    log.Println("Stopping consumer...")
    c.cancel()
    c.wg.Wait()
    
    if err := c.client.Close(); err != nil {
        log.Printf("Error closing consumer: %v", err)
    }
    
    if c.mongoClient != nil {
        if err := c.mongoClient.Disconnect(c.ctx); err != nil {
            log.Printf("Error disconnecting from MongoDB: %v", err)
        }
    }
    
    log.Println("Consumer stopped")
}

// Setup implements sarama.ConsumerGroupHandler
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
    close(c.ready)
    return nil
}

// Cleanup implements sarama.ConsumerGroupHandler
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
    return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for {
        select {
        case message := <-claim.Messages():
            if message == nil {
                return nil
            }
            
            if err := c.processMessage(message); err != nil {
                log.Printf("Error processing message: %v", err)
            } else {
                session.MarkMessage(message, "")
            }

        case <-c.ctx.Done():
            return nil
        }
    }
}

func (c *Consumer) processMessage(msg *sarama.ConsumerMessage) error {
    // Parse message value as JSON if possible, otherwise store as string
    var value interface{}
    if err := json.Unmarshal(msg.Value, &value); err != nil {
        value = string(msg.Value)
    }

    // Convert headers
    headers := make(map[string]string)
    for _, header := range msg.Headers {
        headers[string(header.Key)] = string(header.Value)
    }

    message := Message{
        Topic:     msg.Topic,
        Partition: msg.Partition,
        Offset:    msg.Offset,
        Key:       string(msg.Key),
        Value:     value,
        Headers:   headers,
        Timestamp: msg.Timestamp,
    }

    log.Printf("Consumed message from %s[%d]@%d: %s", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

    // Store in MongoDB if configured
    if c.mongoCollection != nil {
        if err := c.storeMessage(message); err != nil {
            return fmt.Errorf("failed to store message in MongoDB: %w", err)
        }
    }

    return nil
}

func (c *Consumer) storeMessage(msg Message) error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    _, err := c.mongoCollection.InsertOne(ctx, msg)
    if err != nil {
        return err
    }

    log.Printf("Message stored in MongoDB: %s[%d]@%d", msg.Topic, msg.Partition, msg.Offset)
    return nil
}