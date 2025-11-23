package output

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/romeros69/data-streaming-analysis-lab1/internal/config"
	"github.com/romeros69/data-streaming-analysis-lab1/internal/generator"
)

type KafkaOutput struct {
	producer sarama.SyncProducer
	topic    string
}

func NewKafkaOutput(cfg *config.KafkaConfig) (*KafkaOutput, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer(cfg.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	return &KafkaOutput{
		producer: producer,
		topic:    cfg.Topic,
	}, nil
}

func (k *KafkaOutput) Write(entry *generator.LogEntry) error {
	jsonData, err := entry.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal log: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.StringEncoder(jsonData),
	}

	partition, offset, err := k.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message to kafka: %w", err)
	}

	log.Printf("Message sent to partition %d at offset %d", partition, offset)
	return nil
}

func (k *KafkaOutput) Close() error {
	if k.producer != nil {
		return k.producer.Close()
	}
	return nil
}
