package datastream

import (
	"context"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// Sink represents a destination like Kafka or Elastic
type Sink interface {
	Write(ctx context.Context, data []byte) error
}

// GeneratorFunc defines a function that produces a single instance of type T.
type GeneratorFunc[T any] func() T

// MarshalFunc defines a function that converts type T into a byte slice for transport.
type MarshalFunc[T any] func(T) ([]byte, error)

// Engine runs data generation and publishes the generators
type Engine[T any] struct {
	generator func() T
	marshal   func(T) ([]byte, error)
	sink      Sink
	limit     *rate.Limiter
}

type Option[T any] func(*Engine[T])

// NewEngine creates a new Engine with the required components and optional configurations.
func NewEngine[T any](g GeneratorFunc[T], m MarshalFunc[T], sink Sink, opts ...Option[T]) *Engine[T] {
	e := &Engine[T]{
		generator: g,
		marshal:   m,
		sink:      sink,
		limit:     rate.NewLimiter(rate.Inf, 0),
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// WithRateLimit configures the engine to produce at most `eventsPerSecond`.
// The burst parameter allows for sudden spikes up to the specified amount.
func WithRateLimit[T any](eventsPerSecond float64, burst int) Option[T] {
	return func(e *Engine[T]) {
		e.limit = rate.NewLimiter(rate.Limit(eventsPerSecond), burst)
	}
}

// Run starts the data generation, serializes it into the appropriate format,
// and sends the data to the publisher. To cancel the engine running, pass a
// cancellable context to Run.
func (e *Engine[T]) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for {
		// Respect rate limiting
		if err := e.limit.Wait(ctx); err != nil {
			return g.Wait()
		}

		item := e.generator()

		g.Go(func() error {
			b, err := e.marshal(item)
			if err != nil {
				return err
			}
			return e.sink.Write(ctx, b)
		})
	}
}
