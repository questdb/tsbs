package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/questdb/tsbs/pkg/query"
)

// FlightClient is a client for querying InfluxDB v3 via Arrow Flight
type FlightClient struct {
	client   *influxdb3.Client
	database string
}

// NewFlightClient creates a new Flight client for InfluxDB v3
func NewFlightClient(hostURL string, database string, authToken string) (*FlightClient, error) {
	// influxdb3-go requires a token even when server runs without auth
	// Use a placeholder token if none provided
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
		client:   client,
		database: database,
	}, nil
}

// Close closes the Flight client
func (c *FlightClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// Do executes a query and returns latency in milliseconds
func (c *FlightClient) Do(q *query.HTTP, opts *HTTPClientDoOptions) (lag float64, err error) {
	// Extract InfluxQL query from the path
	// Path format: /query?q=SELECT...
	queryStr := string(q.Path)
	if strings.HasPrefix(queryStr, "/query?q=") {
		queryStr = queryStr[9:] // Skip "/query?q="
	} else if strings.HasPrefix(queryStr, "/query?") {
		// Handle other formats
		parts := strings.SplitN(queryStr[7:], "&", 2)
		for _, part := range parts {
			if strings.HasPrefix(part, "q=") {
				queryStr = part[2:]
				break
			}
		}
	}

	// URL decode the query
	queryStr, err = url.QueryUnescape(queryStr)
	if err != nil {
		return 0, fmt.Errorf("failed to unescape query: %w", err)
	}

	ctx := context.Background()

	// Execute query and measure latency
	start := time.Now()

	// Use QueryWithOptions for InfluxQL
	iterator, err := c.client.QueryWithOptions(ctx, &influxdb3.QueryOptions{
		Database: c.database,
		QueryType: influxdb3.InfluxQL,
	}, queryStr)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}

	// Consume all results to ensure we measure full query time
	for iterator.Next() {
		_ = iterator.Value()
	}
	if err := iterator.Err(); err != nil {
		return 0, fmt.Errorf("error reading results: %w", err)
	}

	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

	return lag, nil
}
