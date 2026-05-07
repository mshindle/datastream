package logger

import (
	"context"
	"fmt"
	"io"
)

// SinkFunc adapts a function to the datastream.Sink interface.
type SinkFunc func(ctx context.Context, p []byte) error

func (sf SinkFunc) Write(ctx context.Context, p []byte) error {
	// The function itself should handle context cancellation if it's long-running,
	// but we can check here before invoking as a safeguard.
	if err := ctx.Err(); err != nil {
		return err
	}
	return sf(ctx, p)
}

// Println is a helper function that can be cast to a SinkFunc.
// It writes the byte slice followed by a newline to io.Writer.
func Println(w io.Writer) SinkFunc {
	return func(_ context.Context, p []byte) error {
		_, err := fmt.Fprintln(w, string(p))
		return err
	}
}
