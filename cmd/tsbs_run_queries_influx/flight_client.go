package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/questdb/tsbs/pkg/query"
)

type flightIterator interface {
	Next() bool
	Value() map[string]any
	Err() error
}

type flightQueryExecutor interface {
	QueryWithOptions(context.Context, *influxdb3.QueryOptions, string) (flightIterator, error)
	Close() error
}

type influxFlightQueryExecutor struct{ client *influxdb3.Client }

func (executor *influxFlightQueryExecutor) QueryWithOptions(ctx context.Context, options *influxdb3.QueryOptions, query string) (flightIterator, error) {
	return executor.client.QueryWithOptions(ctx, options, query)
}
func (executor *influxFlightQueryExecutor) Close() error { return executor.client.Close() }

// FlightClient is a client for querying InfluxDB v3 via Arrow Flight.
type FlightClient struct {
	client   flightQueryExecutor
	database string
}

// NewFlightClient creates a new Flight client for InfluxDB v3.
func NewFlightClient(hostURL string, database string, authToken string) (*FlightClient, error) {
	// influxdb3-go requires a token even when server runs without auth.
	token := authToken
	if token == "" {
		token = "unused"
	}
	client, err := influxdb3.New(influxdb3.ClientConfig{Host: hostURL, Database: database, Token: token})
	if err != nil {
		return nil, fmt.Errorf("failed to create InfluxDB3 client: %w", err)
	}
	return &FlightClient{client: &influxFlightQueryExecutor{client: client}, database: database}, nil
}

// Close closes the Flight client.
func (c *FlightClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

func influxQLFromGeneratedPath(path []byte) (string, error) {
	uri, err := url.ParseRequestURI(string(path))
	if err != nil {
		return "", fmt.Errorf("parse generated InfluxDB query path: %w", err)
	}
	if uri.Path != "/query" {
		return "", fmt.Errorf("generated InfluxDB query path is %q, want /query", uri.Path)
	}
	values, ok := uri.Query()["q"]
	if !ok || len(values) != 1 || values[0] == "" {
		return "", errors.New("generated InfluxDB query path must contain exactly one non-empty q parameter")
	}
	return values[0], nil
}

// Do executes a query and returns latency in milliseconds.
func (c *FlightClient) Do(q *query.HTTP, _ *HTTPClientDoOptions) (float64, error) {
	if c == nil || c.client == nil {
		return 0, errors.New("InfluxDB 3 Flight client is nil")
	}
	if q == nil {
		return 0, errors.New("InfluxDB query is nil")
	}
	queryString, err := influxQLFromGeneratedPath(q.Path)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	iterator, err := c.client.QueryWithOptions(context.Background(), &influxdb3.QueryOptions{
		Database:  c.database,
		QueryType: influxdb3.InfluxQL,
	}, queryString)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	for iterator.Next() {
		_ = iterator.Value()
	}
	if err := iterator.Err(); err != nil {
		return 0, fmt.Errorf("error reading results: %w", err)
	}
	return float64(time.Since(start).Nanoseconds()) / 1e6, nil
}
