package elastic

import (
	"bytes"
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esutil"
)

const maxRetries = 5

type Config struct {
	Hosts         []string
	Username      string
	Password      string
	Options       []elasticsearch.Option
	Index         string
	NumWorkers    int
	FlushBytes    int
	FlushInterval time.Duration
}

type Service struct {
	client *elasticsearch.Client
	bi     esutil.BulkIndexer
	count  uint64
}

func New(c Config) (*Service, error) {
	opts := make([]elasticsearch.Option, 1, 5)
	opts[0] = elasticsearch.WithRetry(maxRetries, 429, 502, 503, 504)

	// configure hosts
	if c.Hosts != nil && len(c.Hosts) > 0 {
		opts = append(opts, elasticsearch.WithAddresses(c.Hosts...))
	}
	// see if they provided BasicAuth
	if c.Username != "" || c.Password != "" {
		opts = append(opts, elasticsearch.WithBasicAuth(c.Username, c.Password))
	}
	// Apply any other options they want
	if c.Options != nil {
		opts = append(opts, c.Options...)
	}

	client, err := elasticsearch.New(opts...)
	if err != nil {
		return nil, err
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        client,
		Index:         c.Index,
		NumWorkers:    c.NumWorkers,
		FlushBytes:    c.FlushBytes,
		FlushInterval: c.FlushInterval,
	})
	if err != nil {
		return nil, err
	}

	return &Service{client: client, bi: bi}, err
}

// Write indexes the provided byte slice into Elasticsearch.
// It implements the datastream.Sink interface.
func (s *Service) Write(ctx context.Context, data []byte) error {
	err := s.bi.Add(
		ctx, // Use the context passed into Write, not the one from New()
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "index",

			// Body is an `io.Reader` with the payload
			Body: bytes.NewReader(data),

			// OnSuccess is called for each successful operation
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				atomic.AddUint64(&s.count, 1)
				// We rely on the caller/engine for logging now, or metrics hooks
			},

			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				// Note: esutil.BulkIndexer runs asynchronously. If we want strict error
				// propagation, we might need a channel to pass this back up, or just
				// record it in a metric. For now, we will just let the bulk indexer
				// track the failure stats, which can be inspected later.
			},
		},
	)

	// This error only indicates if the item failed to be *added* to the bulk queue,
	// not if it failed to actually index in Elasticsearch.
	return err
}

func (s *Service) ListIndices() error {
	_, err := s.client.Indices.GetAlias()
	return err
}

func (s *Service) Close(ctx context.Context) error {
	return s.bi.Close(ctx)
}

func simpleExponentialDelay(i int) time.Duration {
	d := time.Duration(math.Exp2(float64(i))) * time.Second
	return d
}
