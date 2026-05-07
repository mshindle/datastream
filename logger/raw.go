package logger

import (
	"context"
	"io"
	"os"
)

// Raw implements the datastream.Sink interface by writing raw bytes to the configured writer - os.Stdout by default.
type Raw struct {
	w io.Writer
}

type Option func(*Raw)

// NewRaw creates a new Raw structure that defaults to writing to os.Stdout.
func NewRaw(opts ...Option) *Raw {
	r := &Raw{w: os.Stdout}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Write implements datastream.Sink. It prints the byte slice followed by a newline.
func (r *Raw) Write(ctx context.Context, data []byte) error {
	// Respect context cancellation
	if err := ctx.Err(); err != nil {
		return err
	}

	_, err := r.w.Write(data)
	return err
}

func WithWriter(w io.Writer) Option {
	return func(raw *Raw) {
		raw.w = w
	}
}
