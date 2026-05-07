# datastream

`datastream` is a high-performance, type-safe Go library for generating, marshaling, and publishing data streams to various destinations (sinks). 

Originally designed for robust load testing and database seeding, `datastream` provides a reliable pipeline architecture out-of-the-box. It manages concurrency, context cancellation, and rate limiting so you can focus strictly on what data you want to create and where you want it to go.

## Features

- **Type-Safe Generation**: Utilizes Go generics to ensure your data models are strongly typed throughout the pipeline.
- **Built-in Rate Limiting**: Control your throughput (events per second) to safely load test infrastructure without causing unintended denial-of-service.
- **Graceful Shutdown**: Fully context-aware. Canceling a context gracefully drains the pipeline and stops background goroutines.
- **Pluggable Sinks**: Ships with built-in sinks for **Kafka**, **Elasticsearch**, and **Standard Output**, while making it trivial to write your own destinations via the `Sink` interface.

## Installation
```bash
go get github.com/mshindle/datastream
```
## Quick Start

This example demonstrates how to create a data pipeline that generates mock user events, marshals them to JSON, and prints them to the console at a controlled rate of 5 events per second.
```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/mshindle/datastream"
	"github.com/mshindle/datastream/logger"
)

type UserEvent struct {
	ID        int       `json:"id"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// Setup context that listens for CTRL+C for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	var counter int

	generator := func() UserEvent {
		counter++
		return UserEvent{
			ID:        counter,
			Action:    "login",
			Timestamp: time.Now(),
		}
	}

	marshaller := func(u UserEvent) ([]byte, error) {
		return json.Marshal(u)
	}

	// Since we want to create an NDJSON file, we use logger.Println which appends a newline
	// after each set of bytes is written.
	sink := logger.Println(os.Stdout)

	// We apply a rate limit of 5 events per second, with a burst capacity of 1.
	engine := datastream.NewEngine[UserEvent](
		generator,
		marshaller,
		sink,
		datastream.WithRateLimit[UserEvent](5.0, 1),
	)

	log.Println("Starting datastream... Press CTRL+C to stop.")

	// 6. Run blocks until an error occurs or the context is cancelled
	if err := engine.Run(ctx); err != nil {
		log.Fatalf("Engine stopped: %v", err)
	}

	log.Println("datastream shut down gracefully.")
}
```
## Concurrency Model & Thread Safety
`datastream` is designed to make writing custom components as simple as possible while maintaining high throughput. To
achieve this, it uses a split execution model:

 1. **Generators are Sequential:** The `GeneratorFunc` is called sequentially in the main event loop. **You do not need to use mutexes or atomic variables** for internal generator state (like incrementing IDs or advancing timestamps).
 1. **Marshallers and Sinks are Concurrent:** Once an item is generated, marshaling and sinking (`Sink.Write`) are dispatched to a concurrent worker pool.
    * Custom `MarshalFunc` implementations must be thread-safe.
    * Custom `Sink` implementations must be thread-safe (e.g., if writing to a shared in-memory buffer, you must use a Mutex).

## Built-in Sinks

`datastream` comes with several production-ready sinks.

### Kafka
Publishes byte payloads synchronously to a Kafka topic.
```go
sink, err := kafka.NewService(kafka.Config{
    BootstrapServers: []string{"localhost:9092"},
    Topic:            "user-events",
})

```
### Elasticsearch
Leverages the official Elasticsearch Bulk Indexer for high-throughput indexing.
```go
sink, err := elastic.New(ctx, elastic.Config{
    Hosts: []string{"http://localhost:9200"},
    Index: "user-events",
})

```
### Custom Sinks
You can write to any destination by implementing the `Sink` interface, or by adapting a simple function using `logger.SinkFunc`:
```go
type Sink interface {
    Write(ctx context.Context, data []byte) error
}

```
