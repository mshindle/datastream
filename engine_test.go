package datastream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

type mockSink struct {
	mu    sync.Mutex
	data  [][]byte
	err   error
	delay time.Duration
}

func (m *mockSink) Write(ctx context.Context, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.data = append(m.data, data)
	return nil
}

func (m *mockSink) getData() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data
}

func TestNewEngine(t *testing.T) {
	g := func() string { return "test" }
	m := func(s string) ([]byte, error) { return []byte(s), nil }
	s := &mockSink{}

	engine := NewEngine(g, m, s)
	if engine.generator == nil {
		t.Error("generator should not be nil")
	}
	if engine.marshal == nil {
		t.Error("marshal should not be nil")
	}
	if engine.sink != s {
		t.Error("sink should be the one provided")
	}
	if engine.limit.Limit() != rate.Inf {
		t.Errorf("expected infinite limit, got %v", engine.limit.Limit())
	}
}

func TestWithRateLimit(t *testing.T) {
	g := func() string { return "test" }
	m := func(s string) ([]byte, error) { return []byte(s), nil }
	s := &mockSink{}

	engine := NewEngine(g, m, s, WithRateLimit[string](10, 5))
	if engine.limit.Limit() != 10 {
		t.Errorf("expected limit 10, got %v", engine.limit.Limit())
	}
	if engine.limit.Burst() != 5 {
		t.Errorf("expected burst 5, got %d", engine.limit.Burst())
	}
}

func TestEngine_Run_Success(t *testing.T) {
	g := func() string { return "data" }
	m := func(s string) ([]byte, error) { return []byte(s), nil }
	sink := &mockSink{}

	engine := NewEngine(g, m, sink)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := engine.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("unexpected error: %v", err)
	}

	data := sink.getData()
	if len(data) == 0 {
		t.Error("expected to have written some data, but got none")
	}

	for _, d := range data {
		if string(d) != "data" {
			t.Errorf("expected 'data', got %s", string(d))
		}
	}
}

func TestEngine_Run_MarshalError(t *testing.T) {
	g := func() string { return "data" }
	marshalErr := errors.New("marshal error")
	m := func(s string) ([]byte, error) { return nil, marshalErr }
	sink := &mockSink{}

	engine := NewEngine(g, m, sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := engine.Run(ctx)
	if !errors.Is(err, marshalErr) {
		t.Errorf("expected marshal error, got %v", err)
	}
}

func TestEngine_Run_SinkError(t *testing.T) {
	g := func() string { return "data" }
	m := func(s string) ([]byte, error) { return []byte(s), nil }
	sinkErr := errors.New("sink error")
	sink := &mockSink{err: sinkErr}

	engine := NewEngine(g, m, sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := engine.Run(ctx)
	if !errors.Is(err, sinkErr) {
		t.Errorf("expected sink error, got %v", err)
	}
}

func TestEngine_Run_ContextCancel(t *testing.T) {
	g := func() string { return "data" }
	m := func(s string) ([]byte, error) { return []byte(s), nil }
	sink := &mockSink{}

	engine := NewEngine(g, m, sink)

	ctx, cancel := context.WithCancel(context.Background())
	// Run engine in a goroutine and cancel context after a short while
	errCh := make(chan error)
	go func() {
		errCh <- engine.Run(ctx)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled or nil, got %v", err)
	}
}

func TestEngine_Run_RateLimiting(t *testing.T) {
	g := func() string { return "data" }
	m := func(s string) ([]byte, error) { return []byte(s), nil }
	sink := &mockSink{}

	// 100 events per second, with burst to allow initial events
	engine := NewEngine(g, m, sink, WithRateLimit[string](100, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 105*time.Millisecond)
	defer cancel()

	_ = engine.Run(ctx)

	count := len(sink.getData())
	// In 100ms at 100 events/sec, we expect around 10 events.
	// 105ms should give roughly 11.
	if count < 8 || count > 15 {
		t.Errorf("expected around 10 events, got %d", count)
	}
}
