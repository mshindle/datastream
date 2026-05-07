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
