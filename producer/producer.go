package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

var (
	kafkaBrokers = []string{
		"localhost:8097",
		"localhost:8098",
		"localhost:8099",
	}
	kafkaTopic = "random-words"
	prefixes   = []string{"funny", "happy", "sad", "angry", "excited", "calm", "peaceful", "scared", "tired", "bored", "stressed", "relaxed", "confused", "surprised", "proud", "jealous", "guilty", "embarrassed", "annoyed", "disappointed", "hopeful", "lonely", "loved", "hated", "rejected", "accepted", "optimistic", "pessimistic", "courageous", "cowardly", "energetic", "lazy", "enthusiastic", "passionate", "indifferent", "sympathetic", "empathetic", "selfish", "generous", "greedy", "thrifty", "patient", "impatient", "determined", "indecisive", "reliable", "unreliable", "responsible", "irresponsible", "honest", "dishonest", "loyal", "disloyal", "trustworthy", "untrustworthy", "kind", "cruel", "polite", "rude", "friendly", "hostile", "sincere", "insincere", "sensible", "sensitive", "serious", "silly", "mature", "immature", "optimistic", "pessimistic", "confident", "shy", "brave", "cowardly", "humble", "proud", "modest", "arrogant", "generous", "greedy", "thrifty", "patient", "impatient", "determined", "indecisive", "reliable", "unreliable", "responsible", "irresponsible", "honest", "dishonest", "loyal", "disloyal", "trustworthy", "untrustworthy", "kind", "cruel", "polite", "rude", "friendly", "hostile", "sincere", "insincere", "sensible", "sensitive", "serious", "silly", "mature", "immature", "optimistic", "pessimistic", "confident", "shy", "brave", "cowardly", "humble", "proud", "modest", "arrogant", "generous", "greedy", "thrifty", "patient", "impatient", "determined", "indecisive", "reliable"}
	names      = []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Mallory", "Oscar", "Peggy", "Trent", "Walter", "Zara"}
)

func main() {
	// Create a new producer
	producer := NewProducer()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// Start the producer
		producer.Start()
	}()

	<-signalChan
	log.Println("Received shutdown signal, shutting down gracefully...")
	producer.writer.Close()
	log.Println("Producer shutdown successfully")
}

func NewProducer() *Producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokers...),
		Topic:                  kafkaTopic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
	}
	return &Producer{
		writer: writer,
	}
}

func (p *Producer) Start() {
	// Start the producer
	for {
		message := kafka.Message{
			Value: []byte(randomWord()),
		}
		err := p.writer.WriteMessages(context.Background(), message)
		if err != nil {
			log.Printf("could not write message %v", err)
		}
		log.Printf("Message sent:[%s] to kafka topic: [%s] | partition: [%v] | offset: [%v]", string(message.Value), kafkaTopic, message.Partition, message.Offset)
		time.Sleep(5 * time.Second)
	}
}

func randomWord() string {
	// Return a random word from the word list
	return prefixes[rand.Intn(len(prefixes))] + " " + names[rand.Intn(len(names))]
}
