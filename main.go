package amqp_helper

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	types_plugin "github.com/CodeClarityCE/utility-types/plugin_db"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Client maintains a persistent AMQP connection with channel and queue declaration caching.
type Client struct {
	conn           *amqp.Connection
	mu             sync.Mutex
	channels       map[string]*amqp.Channel
	declaredQueues map[string]bool
}

var (
	defaultClient     *Client
	defaultClientOnce sync.Once
	defaultClientErr  error
)

// buildURL constructs the AMQP URL from environment variables.
func buildURL() string {
	protocol := os.Getenv("AMQP_PROTOCOL")
	if protocol == "" {
		protocol = "amqp"
	}
	host := os.Getenv("AMQP_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("AMQP_PORT")
	if port == "" {
		port = "5672"
	}
	user := os.Getenv("AMQP_USER")
	if user == "" {
		user = "guest"
	}
	password := os.Getenv("AMQP_PASSWORD")
	if password == "" {
		password = "guest"
	}
	return protocol + "://" + user + ":" + password + "@" + host + ":" + port + "/"
}

// getSSLMode returns the configured AMQP SSL mode from AMQP_SSLMODE, defaulting
// based on ENV (mirrors the PostgreSQL helper's behaviour).
func getSSLMode() string {
	sslMode := os.Getenv("AMQP_SSLMODE")
	if sslMode != "" {
		return sslMode
	}
	env := os.Getenv("ENV")
	if env == "prod" || env == "production" {
		return "verify-ca"
	}
	return "disable"
}

// buildTLSConfig builds a *tls.Config from AMQP_SSLMODE / AMQP_SSLROOTCERT.
// Returns nil when TLS verification is not required (plain "require" still gets
// an InsecureSkipVerify config; "disable" returns nil).
func buildTLSConfig() *tls.Config {
	switch getSSLMode() {
	case "require":
		return &tls.Config{InsecureSkipVerify: true}
	case "verify-ca", "verify-full":
		cfg := &tls.Config{}
		if host := os.Getenv("AMQP_HOST"); host != "" {
			cfg.ServerName = host
		}
		if rootCert := os.Getenv("AMQP_SSLROOTCERT"); rootCert != "" {
			if caCert, err := os.ReadFile(rootCert); err == nil {
				pool := x509.NewCertPool()
				pool.AppendCertsFromPEM(caCert)
				cfg.RootCAs = pool
			}
		}
		return cfg
	default:
		return nil
	}
}

// Dial opens an AMQP connection, using TLS when the URL scheme is "amqps".
// The TLS configuration is derived from AMQP_SSLMODE / AMQP_SSLROOTCERT.
func Dial(url string) (*amqp.Connection, error) {
	if strings.HasPrefix(url, "amqps://") {
		if cfg := buildTLSConfig(); cfg != nil {
			return amqp.DialTLS(url, cfg)
		}
	}
	return amqp.Dial(url)
}

// NewClient creates a new Client with a persistent AMQP connection.
func NewClient(url string) (*Client, error) {
	conn, err := Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	return &Client{
		conn:           conn,
		channels:       make(map[string]*amqp.Channel),
		declaredQueues: make(map[string]bool),
	}, nil
}

// getDefaultClient returns the lazily-initialized singleton client.
func getDefaultClient() (*Client, error) {
	defaultClientOnce.Do(func() {
		defaultClient, defaultClientErr = NewClient(buildURL())
	})
	return defaultClient, defaultClientErr
}

// getOrCreateChannel returns a cached channel for the queue, or creates a new one.
// Must be called with c.mu held.
func (c *Client) getOrCreateChannel(queueName string) (*amqp.Channel, error) {
	ch, exists := c.channels[queueName]
	if exists && ch != nil {
		return ch, nil
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}
	c.channels[queueName] = ch
	return ch, nil
}

// ensureQueueDeclared declares the queue if not already declared on this channel.
// Must be called with c.mu held.
func (c *Client) ensureQueueDeclared(ch *amqp.Channel, queueName string) error {
	if c.declaredQueues[queueName] {
		return nil
	}

	_, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}
	c.declaredQueues[queueName] = true
	return nil
}

// invalidateChannel removes a channel from the cache and clears its queue declaration.
// Must be called with c.mu held.
func (c *Client) invalidateChannel(queueName string) {
	if ch, exists := c.channels[queueName]; exists && ch != nil {
		ch.Close()
	}
	delete(c.channels, queueName)
	delete(c.declaredQueues, queueName)
}

// SendToQueue publishes a message to the given queue using the persistent connection.
// Thread-safe: uses a mutex to protect channel access.
func (c *Client) SendToQueue(queueName string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Try to send, retry once on channel error
	for attempt := 0; attempt < 2; attempt++ {
		ch, err := c.getOrCreateChannel(queueName)
		if err != nil {
			if attempt == 0 {
				c.invalidateChannel(queueName)
				continue
			}
			return err
		}

		if err := c.ensureQueueDeclared(ch, queueName); err != nil {
			if attempt == 0 {
				c.invalidateChannel(queueName)
				continue
			}
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = ch.PublishWithContext(ctx,
			"",        // exchange
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        data,
			})
		cancel()

		if err != nil {
			if attempt == 0 {
				c.invalidateChannel(queueName)
				continue
			}
			return fmt.Errorf("failed to publish message: %w", err)
		}

		return nil
	}

	return fmt.Errorf("failed to send message after retries")
}

// Close closes all channels and the connection.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for name, ch := range c.channels {
		if ch != nil {
			ch.Close()
		}
		delete(c.channels, name)
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// Listen starts consuming messages from the given queue. This blocks forever.
// It maintains its own persistent connection for consuming.
func Listen(queue string, callback func(args any, config types_plugin.Plugin, message []byte), args any, config types_plugin.Plugin) {
	url := buildURL()

	conn, err := Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan struct{})
	go func(callback func(args any, config types_plugin.Plugin, message []byte), args any, config types_plugin.Plugin) {
		for d := range msgs {
			callback(args, config, []byte(d.Body))
		}
	}(callback, args, config)

	log.Printf(" [*] %s Waiting for messages on %s. To exit press CTRL+C", config.Name, queue)
	<-forever
}

// Send publishes a message to the given queue using a persistent singleton connection.
// This is backward-compatible with the original API but now reuses the connection
// instead of creating a new TCP connection per call.
func Send(connection string, data []byte) {
	log.Println("Sending message to " + connection)

	client, err := getDefaultClient()
	failOnError(err, "Failed to get AMQP client")

	err = client.SendToQueue(connection, data)
	failOnError(err, "Failed to send message")

	log.Printf(" [x] Sent %s\n", data)
}
