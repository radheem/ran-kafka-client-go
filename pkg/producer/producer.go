package producer

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type Config struct {
    Brokers []string
}

type Producer struct {
    config Config
    client sarama.SyncProducer
}

type Message struct {
    Key     string            `json:"key,omitempty"`
    Value   interface{}       `json:"value"`
    Headers map[string]string `json:"headers,omitempty"`
}

func NewProducer(config Config) (*Producer, error) {
    // Setup Sarama configuration
    saramaConfig := sarama.NewConfig()
    saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
    saramaConfig.Producer.Retry.Max = 3
    saramaConfig.Producer.Return.Successes = true
    saramaConfig.Producer.Compression = sarama.CompressionSnappy
    saramaConfig.Producer.Flush.Frequency = 500 * time.Millisecond

    client, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
    if err != nil {
        return nil, fmt.Errorf("failed to create producer: %w", err)
    }

    return &Producer{
        config: config,
        client: client,
    }, nil
}

func (p *Producer) SendMessage(topic string, msg Message) error {
    // Convert message value to JSON
    valueBytes, err := json.Marshal(msg.Value)
    if err != nil {
        return fmt.Errorf("failed to marshal message value: %w", err)
    }

    // Convert headers
    var headers []sarama.RecordHeader
    for k, v := range msg.Headers {
        headers = append(headers, sarama.RecordHeader{
            Key:   []byte(k),
            Value: []byte(v),
        })
    }

    producerMsg := &sarama.ProducerMessage{
        Topic:   topic,
        Key:     sarama.StringEncoder(msg.Key),
        Value:   sarama.ByteEncoder(valueBytes),
        Headers: headers,
    }

    partition, offset, err := p.client.SendMessage(producerMsg)
    if err != nil {
        return fmt.Errorf("failed to send message: %w", err)
    }

    log.Printf("Message sent to %s[%d]@%d: %s", topic, partition, offset, string(valueBytes))
    return nil
}

func (p *Producer) SendRawMessage(topic, key string, value []byte, headers map[string]string) error {
    // Convert headers
    var recordHeaders []sarama.RecordHeader
    for k, v := range headers {
        recordHeaders = append(recordHeaders, sarama.RecordHeader{
            Key:   []byte(k),
            Value: []byte(v),
        })
    }

    producerMsg := &sarama.ProducerMessage{
        Topic:   topic,
        Key:     sarama.StringEncoder(key),
        Value:   sarama.ByteEncoder(value),
        Headers: recordHeaders,
    }

    partition, offset, err := p.client.SendMessage(producerMsg)
    if err != nil {
        return fmt.Errorf("failed to send message: %w", err)
    }

    log.Printf("Raw message sent to %s[%d]@%d: %s", topic, partition, offset, string(value))
    return nil
}

func (p *Producer) Close() error {
    return p.client.Close()
}