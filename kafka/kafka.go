package kafka

import (
	"context"

	"github.com/Shopify/sarama"
)

type Config struct {
	BootstrapServers []string
	Username         string
	Password         string
	Topic            string
}

type Service struct {
	topic    string
	producer sarama.SyncProducer
}

// New creates a new Kafka service that implements datastream.Sink.
func New(c Config) (*Service, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// If SASL authentication is provided in the config
	if c.Username != "" && c.Password != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.Username
		config.Net.SASL.Password = c.Password
	}

	producer, err := sarama.NewSyncProducer(c.BootstrapServers, config)
	if err != nil {
		return nil, err
	}

	return &Service{
		producer: producer,
		topic:    c.Topic,
	}, nil
}

// Write implements the datastream.Sink interface. It blocks until the message is acknowledged
// by the Kafka cluster or an error occurs.
func (s *Service) Write(ctx context.Context, data []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: s.topic,
		Value: sarama.ByteEncoder(data),
	}

	// Wait for context cancellation or publish the message
	errChan := make(chan error, 1)
	go func() {
		_, _, err := s.producer.SendMessage(msg)
		errChan <- err
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

// Close gracefully shuts down the Kafka producer.
func (s *Service) Close() error {
	if s.producer != nil {
		return s.producer.Close()
	}
	return nil
}
