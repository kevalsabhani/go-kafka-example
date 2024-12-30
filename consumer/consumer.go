package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaBrokers = []string{
		"localhost:8097",
		"localhost:8098",
		"localhost:8099",
	}
	kafkaTopic         = "random-words"
	kafkaConsumerGroup = "my-consumer-group"
)

func main() {
	// Create a new consumer
	consumer := NewConsumer()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		// Start the consumer
		consumer.Start()
	}()

	<-signalChan
	log.Println("Received shutdown signal, shutting down gracefully...")
	consumer.reader.Close()
	log.Println("Consumer shutdown successfully")
}

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer() *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: kafkaBrokers,
		Topic:   kafkaTopic,
		GroupID: kafkaConsumerGroup,
	})
	return &Consumer{
		reader: reader,
	}
}

func (c *Consumer) Start() {
	// Start the consumer
	for {
		m, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("could not read message %v", err)
		}
		log.Printf("Message received: [%s] from kafka topic: [%s] | partition: [%v] | offset: [%v]", string(m.Value), kafkaTopic, m.Partition, m.Offset)
	}
}
