package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/questdb/tsbs/pkg/query"
)

type rawFlightReader interface {
	Next() bool
	Err() error
	Release()
}

type flightQueryClient interface {
	QueryWithOptions(context.Context, *influxdb3.QueryOptions, string) (rawFlightReader, error)
	Close() error
}

type influxDB3ClientAdapter struct {
	client *influxdb3.Client
}

func (c *influxDB3ClientAdapter) QueryWithOptions(ctx context.Context, options *influxdb3.QueryOptions, queryText string) (rawFlightReader, error) {
	iterator, err := c.client.QueryWithOptions(ctx, options, queryText)
	if err != nil {
		return nil, err
	}
	if iterator == nil {
		return nil, fmt.Errorf("query returned a nil iterator")
	}
	reader := iterator.Raw()
	if reader == nil {
		return nil, fmt.Errorf("query returned a nil raw reader")
	}
	return reader, nil
}

func (c *influxDB3ClientAdapter) Close() error { return c.client.Close() }

// FlightClient is a client for querying InfluxDB v3 via Arrow Flight.
type FlightClient struct {
	client   flightQueryClient
	database string
}

// NewFlightClient creates a new Flight client for InfluxDB v3.
func NewFlightClient(hostURL string, database string, authToken string) (*FlightClient, error) {
	// influxdb3-go requires a token even when server runs without auth.
	token := authToken
	if token == "" {
		token = "unused"
	}
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     hostURL,
		Database: database,
		Token:    token,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create InfluxDB3 client: %w", err)
	}

	return &FlightClient{
		client:   &influxDB3ClientAdapter{client: client},
		database: database,
	}, nil
}

// Close closes the Flight client.
func (c *FlightClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// Do executes a query and returns latency in milliseconds.
func (c *FlightClient) Do(q *query.HTTP, opts *HTTPClientDoOptions) (lag float64, err error) {
	if c == nil || c.client == nil {
		return 0, errors.New("InfluxDB 3 Flight client is nil")
	}
	if q == nil {
		return 0, errors.New("InfluxDB query is nil")
	}
	parsed, err := url.ParseRequestURI(string(q.Path))
	if err != nil || parsed.Path != "/query" {
		return 0, fmt.Errorf("invalid query path %q", q.Path)
	}
	values, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return 0, fmt.Errorf("invalid query parameters: %w", err)
	}
	queries := values["q"]
	if len(queries) != 1 || queries[0] == "" {
		return 0, fmt.Errorf("query path must contain exactly one non-empty q parameter")
	}

	start := time.Now()
	reader, err := c.client.QueryWithOptions(context.Background(), &influxdb3.QueryOptions{
		Database:  c.database,
		QueryType: influxdb3.InfluxQL,
	}, queries[0])
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	if reader == nil {
		return 0, fmt.Errorf("query returned a nil raw reader")
	}
	defer reader.Release()

	for reader.Next() {
	}
	if err := reader.Err(); err != nil {
		return 0, fmt.Errorf("error reading results: %w", err)
	}
	lag = float64(time.Since(start).Nanoseconds()) / 1e6

	if opts != nil {
		switch opts.Debug {
		case 1:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms\n", q.HumanLabel, lag)
		case 2:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		case 3:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
			fmt.Fprintf(os.Stderr, "debug:   request: %s\n", q.String())
		}
	}

	return lag, nil
}
